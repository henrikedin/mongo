/*    Copyright 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/client/dbclient_cursor_network.h"

#include "mongo/client/connpool.h"
#include "mongo/db/client.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/getmore_request.h"
#include "mongo/db/query/query_request.h"
#include "mongo/rpc/factory.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/rpc/metadata.h"
#include "mongo/rpc/object_check.h"
#include "mongo/s/stale_exception.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/bufreader.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/destructor_guard.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

using std::unique_ptr;
using std::endl;
using std::string;
using std::vector;

namespace {
Message assembleCommandRequest(DBClientBase* cli,
                               StringData database,
                               int legacyQueryOptions,
                               BSONObj legacyQuery) {
    auto request = rpc::upconvertRequest(database, std::move(legacyQuery), legacyQueryOptions);

    if (cli->getRequestMetadataWriter()) {
        BSONObjBuilder bodyBob(std::move(request.body));
        auto opCtx = (haveClient() ? cc().getOperationContext() : nullptr);
        uassertStatusOK(cli->getRequestMetadataWriter()(opCtx, &bodyBob));
        request.body = bodyBob.obj();
    }

    return rpc::messageFromOpMsgRequest(
        cli->getClientRPCProtocols(), cli->getServerRPCProtocols(), std::move(request));
}

}  // namespace

void DBClientCursorNetwork::requestMore() {
    if (opts & QueryOption_Exhaust) {
        return exhaustReceiveMore();
    }

    invariant(!_connectionHasPendingReplies);
    verify(cursorId && batch.pos == batch.objs.size());

    if (haveLimit) {
        nToReturn -= batch.objs.size();
        verify(nToReturn > 0);
    }

    ON_BLOCK_EXIT([ this, origClient = _client ] { _client = origClient; });
    boost::optional<ScopedDbConnection> connHolder;
    if (!_client) {
        invariant(_scopedHost.size());
        connHolder.emplace(_scopedHost);
        _client = connHolder->get();
    }

    Message toSend = _assembleGetMore();
    Message response;
    _client->call(toSend, response);

    // If call() succeeds, the connection is clean so we can return it to the pool, even if
    // dataReceived() throws because the command reported failure. However, we can't return it yet,
    // because dataReceived() needs to get the metadata reader from the connection.
    ON_BLOCK_EXIT([&] {
        if (connHolder)
            connHolder->done();
    });

    dataReceived(response);
}

/** with QueryOption_Exhaust, the server just blasts data at us (marked at end with cursorid==0). */
void DBClientCursorNetwork::exhaustReceiveMore() {
    verify(cursorId && batch.pos == batch.objs.size());
    uassert(40675, "Cannot have limit for exhaust query", !haveLimit);
    Message response;
    verify(_client);
    if (!_client->recv(response, _lastRequestId)) {
        uasserted(16465, "recv failed while exhausting cursor");
    }
    dataReceived(response);
}

void DBClientCursorNetwork::attach(AScopedConnection* conn) {
    verify(_scopedHost.size() == 0);
    verify(conn);
    verify(conn->get());

    if (conn->get()->type() == ConnectionString::SET) {
        if (_lazyHost.size() > 0)
            _scopedHost = _lazyHost;
        else if (_client)
            _scopedHost = _client->getServerAddress();
        else
            massert(14821,
                    "No client or lazy client specified, cannot store multi-host connection.",
                    false);
    } else {
        _scopedHost = conn->getHost();
    }

    conn->done();
    _client = 0;
    _lazyHost = "";
}

DBClientCursorNetwork::DBClientCursorNetwork(DBClientNetwork* client,
                                             const std::string& ns,
                                             const BSONObj& query,
                                             int nToReturn,
                                             int nToSkip,
                                             const BSONObj* fieldsToReturn,
                                             int queryOptions,
                                             int batchSize)
    : DBClientCursor(client,
                     ns,
                     query,
                     0,  // cursorId
                     nToReturn,
                     nToSkip,
                     fieldsToReturn,
                     queryOptions,
                     batchSize) {}

DBClientCursorNetwork::DBClientCursorNetwork(DBClientNetwork* client,
                                             const std::string& ns,
                                             long long cursorId,
                                             int nToReturn,
                                             int queryOptions)
    : DBClientCursor(client,
                     ns,
                     BSONObj(),  // query
                     cursorId,
                     nToReturn,
                     0,        // nToSkip
                     nullptr,  // fieldsToReturn
                     queryOptions,
                     0) {}  // batchSize

DBClientCursorNetwork::~DBClientCursorNetwork() {
    kill_impl();
}

void DBClientCursorNetwork::kill() {
    kill_impl();
}

void DBClientCursorNetwork::kill_impl() {
    DESTRUCTOR_GUARD({
        if (cursorId && _ownCursor && !globalInShutdownDeprecated()) {
            auto killCursor = [&](auto& conn) {
                if (_useFindCommand) {
                    conn->killCursor(ns, cursorId);
                } else {
                    auto toSend = makeKillCursorsMessage(cursorId);
                    conn->say(toSend);
                }
            };

            if (_client && !_connectionHasPendingReplies) {
                killCursor(_client);
            } else {
                // Use a side connection to send the kill cursor request.
                verify(_scopedHost.size() || (_client && _connectionHasPendingReplies));
                ScopedDbConnection conn(_client ? _client->getServerAddress() : _scopedHost);
                killCursor(conn);
                conn.done();
            }
        }
    });

    // Mark this cursor as dead since we can't do any getMores.
    cursorId = 0;
}

}  // namespace mongo
