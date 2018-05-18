/**
 *    Copyright (C) 2008-2015 MongoDB Inc.
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

#include <cstdint>

#include "mongo/base/string_data.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/index_spec.h"
#include "mongo/client/mongo_uri.h"
#include "mongo/client/query.h"
#include "mongo/client/read_preference.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/write_concern_options.h"
#include "mongo/logger/log_severity.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/rpc/message.h"
#include "mongo/rpc/metadata.h"
#include "mongo/rpc/op_msg.h"
#include "mongo/rpc/protocol.h"
#include "mongo/rpc/unique_message.h"
#include "mongo/stdx/functional.h"
#include "mongo/transport/message_compressor_manager.h"
#include "mongo/transport/session.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

namespace executor {
struct RemoteCommandResponse;
}

class DBClientCursor;
class DBClientCursorBatchIterator;

/**
 * Represents a full query description, including all options required for the query to be passed on
 * to other hosts
 */
class QuerySpec {
    std::string _ns;
    int _ntoskip;
    int _ntoreturn;
    int _options;
    BSONObj _query;
    BSONObj _fields;
    Query _queryObj;

public:
    QuerySpec(const std::string& ns,
              const BSONObj& query,
              const BSONObj& fields,
              int ntoskip,
              int ntoreturn,
              int options)
        : _ns(ns),
          _ntoskip(ntoskip),
          _ntoreturn(ntoreturn),
          _options(options),
          _query(query.getOwned()),
          _fields(fields.getOwned()),
          _queryObj(_query) {}

    QuerySpec() {}

    bool isEmpty() const {
        return _ns.size() == 0;
    }

    bool isExplain() const {
        return _queryObj.isExplain();
    }
    BSONObj filter() const {
        return _queryObj.getFilter();
    }

    BSONObj hint() const {
        return _queryObj.getHint();
    }
    BSONObj sort() const {
        return _queryObj.getSort();
    }
    BSONObj query() const {
        return _query;
    }
    BSONObj fields() const {
        return _fields;
    }
    BSONObj* fieldsData() {
        return &_fields;
    }

    // don't love this, but needed downstrem
    const BSONObj* fieldsPtr() const {
        return &_fields;
    }

    std::string ns() const {
        return _ns;
    }
    int ntoskip() const {
        return _ntoskip;
    }
    int ntoreturn() const {
        return _ntoreturn;
    }
    int options() const {
        return _options;
    }

    void setFields(BSONObj& o) {
        _fields = o.getOwned();
    }

    std::string toString() const {
        return str::stream() << "QSpec "
                             << BSON("ns" << _ns << "n2skip" << _ntoskip << "n2return" << _ntoreturn
                                          << "options"
                                          << _options
                                          << "query"
                                          << _query
                                          << "fields"
                                          << _fields);
    }
};


/** Typically one uses the QUERY(...) macro to construct a Query object.
    Example: QUERY( "age" << 33 << "school" << "UCLA" )
*/
#define QUERY(x) ::mongo::Query(BSON(x))

}  // namespace mongo

