/*    Copyright 2009 10gen Inc.
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

#include "mongo/client/dbclient_util.h"

namespace mongo {
BSONElement getErrField(const BSONObj& o) {
    return o["$err"];
}

bool hasErrField(const BSONObj& o) {
    return !getErrField(o).eoo();
}

/** @return the database name portion of an ns string */
std::string nsGetDB(const std::string& ns) {
    std::string::size_type pos = ns.find(".");
    if (pos == std::string::npos)
        return ns;

    return ns.substr(0, pos);
}

/** @return the collection name portion of an ns string */
std::string nsGetCollection(const std::string& ns) {
    std::string::size_type pos = ns.find(".");
    if (pos == std::string::npos)
        return "";

    return ns.substr(pos + 1);
}
}