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
                               long long cursorId,
                               int nToReturn,
                               int nToSkip,
                               const BSONObj* fieldsToReturn,
                               int queryOptions,
                               int bs)
    : DBClientCursor(
          client, ns, query, cursorId, nToReturn, nToSkip, fieldsToReturn, queryOptions, bs) {
    invariant(!(opts & QueryOption_Exhaust));
}

DBDirectCursor::DBDirectCursor(DBDirectClient* client,
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
                     0)  // batchSize
{
    invariant(!(opts & QueryOption_Exhaust));
}

DBDirectCursor::~DBDirectCursor() {
    kill_impl();
}

void DBDirectCursor::kill() {
    kill_impl();
}

void DBDirectCursor::kill_impl() {
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

            invariant(!_connectionHasPendingReplies);
            if (_client) {
                killCursor(_client);
            }
        }
    });

    // Mark this cursor as dead since we can't do any getMores.
    cursorId = 0;
}

void DBDirectCursor::requestMore() {
    invariant(!_connectionHasPendingReplies);
    verify(cursorId && batch.pos == batch.objs.size());

    if (haveLimit) {
        nToReturn -= batch.objs.size();
        verify(nToReturn > 0);
    }

    Message toSend = _assembleGetMore();
    Message response;
    _client->call(toSend, response);

    dataReceived(response);
}
}
