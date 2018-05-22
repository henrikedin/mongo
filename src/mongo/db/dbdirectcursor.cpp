/**
 *    Copyright (C) 2018 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/db/dbdirectcursor.h"

#include "mongo/db/dbdirectclient.h"
#include "mongo/util/destructor_guard.h"
#include "mongo/util/exit.h"

namespace mongo {
DBDirectCursor::DBDirectCursor(DBDirectClient* client,
                               const std::string& ns,
                               const BSONObj& query,
                               int nToReturn,
                               int nToSkip,
                               const BSONObj* fieldsToReturn,
                               int queryOptions,
                               int bs)
    : DBClientCursor(client, ns, query, 0, nToReturn, nToSkip, fieldsToReturn, queryOptions, bs) {
    invariant(!(opts & QueryOption_Exhaust));
}

DBDirectCursor::~DBDirectCursor() {
    kill_direct();
}

void DBDirectCursor::kill() {
    kill_direct();
}

void DBDirectCursor::kill_direct() {
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
                invariant(false);
            }
        }
    });

    // Mark this cursor as dead since we can't do any getMores.
    cursorId = 0;
}

void DBDirectCursor::requestMore() {
    if (opts & QueryOption_Exhaust) {
        // return exhaustReceiveMore();
        invariant(false);
    }

    invariant(!_connectionHasPendingReplies);
    verify(cursorId && batch.pos == batch.objs.size());

    if (haveLimit) {
        nToReturn -= batch.objs.size();
        verify(nToReturn > 0);
    }

    ON_BLOCK_EXIT([ this, origClient = _client ] { _client = origClient; });
    // boost::optional<ScopedDbConnection> connHolder;
    if (!_client) {
        // invariant(_scopedHost.size());
        // connHolder.emplace(_scopedHost);
        //_client = connHolder->get();
        invariant(false);
    }

    Message toSend = _assembleGetMore();
    Message response;
    _client->call(toSend, response);

    // If call() succeeds, the connection is clean so we can return it to the pool, even if
    // dataReceived() throws because the command reported failure. However, we can't return it yet,
    // because dataReceived() needs to get the metadata reader from the connection.
    /*ON_BLOCK_EXIT([&] {
        if (connHolder)
            connHolder->done();
    });*/

    dataReceived(response);
}
}