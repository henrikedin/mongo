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
*    must comply with the GNU Affero General Public License in all respects
*    for all of the code used other than as permitted herein. If you modify
*    file(s) with this exception, you may extend this exception to your
*    version of the file(s), but you are not obligated to do so. If you do not
*    wish to do so, delete this exception statement from your version. If you
*    delete this exception statement from all source files in the program,
*    then also delete it in the license file.
*/

#include "mongo/client/dbclient_network.h"

#include "mongo/client/dbclient_cursor_network.h"

namespace mongo {
std::unique_ptr<DBClientCursorNetwork> DBClientNetwork::query_internal(
    const std::string& ns,
    Query query,
    int nToReturn,
    int nToSkip,
    const BSONObj* fieldsToReturn,
    int queryOptions,
    int batchSize) {
    std::unique_ptr<DBClientCursorNetwork> c(new DBClientCursorNetwork(
        this, ns, query.obj, nToReturn, nToSkip, fieldsToReturn, queryOptions, batchSize));
    if (c->init())
        return c;
    return nullptr;
}

std::unique_ptr<DBClientCursor> DBClientNetwork::query(const std::string& ns,
                                                       Query query,
                                                       int nToReturn,
                                                       int nToSkip,
                                                       const BSONObj* fieldsToReturn,
                                                       int queryOptions,
                                                       int batchSize) {
    return std::unique_ptr<DBClientCursor>(
        query_internal(ns, query, nToReturn, nToSkip, fieldsToReturn, queryOptions, batchSize)
            .release());
}

std::unique_ptr<DBClientCursor> DBClientNetwork::getMore(const std::string& ns,
                                                         long long cursorId,
                                                         int nToReturn,
                                                         int options) {
    std::unique_ptr<DBClientCursor> c(
        new DBClientCursorNetwork(this, ns, cursorId, nToReturn, options));
    if (c->init())
        return c;
    return nullptr;
}

}  //  namespace mongo