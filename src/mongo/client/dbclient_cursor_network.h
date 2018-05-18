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

#pragma once

#include <stack>

#include "mongo/base/disallow_copying.h"
#include "mongo/client/dbclient_base.h"
#include "mongo/client/dbclient_cursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/rpc/message.h"

namespace mongo {

class AScopedConnection;

/** Queries return a cursor object */
class DBClientCursorNetwork : public DBClientCursor {
public:
    DBClientCursorNetwork(DBClientNetwork* client,
                          const std::string& ns,
                          const BSONObj& query,
                          int nToReturn,
                          int nToSkip,
                          const BSONObj* fieldsToReturn,
                          int queryOptions,
                          int bs);

    DBClientCursorNetwork(DBClientNetwork* client,
                          const std::string& ns,
                          long long cursorId,
                          int nToReturn,
                          int options);

    virtual ~DBClientCursorNetwork();

    /**
     * For exhaust. Used in DBClientConnection.
     */
    void exhaustReceiveMore();

    /**
     * Marks this object as dead and sends the KillCursors message to the server.
     *
     * Any errors that result from this are swallowed since this is typically performed as part of
     * cleanup and a failure to kill the cursor should not result in a failure of the operation
     * using the cursor.
     *
     * Killing an already killed or exhausted cursor does nothing, so it is safe to always call this
     * if you want to ensure that a cursor is killed.
     */
    void kill() override;

    void attach(AScopedConnection* conn);

private:
    void kill_impl();

    std::string _scopedHost;

    void requestMore() override;
};

}  // namespace mongo
