/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <sqlite3.h>
#include <string>

#include "mongo/db/storage/mobile/mobile_session.h"
#include "mongo/platform/atomic_word.h"

namespace mongo {

/**
 * SqliteStatement is a wrapper around the sqlite3_stmt object. All calls to the SQLite API that
 * involve a sqlite_stmt object are made in this class.
 */
class SqliteStatement final {
public:
    /**
     * Creates and prepares a SQLite statement.
     */
    template <class... Args>
    SqliteStatement(const MobileSession& session, Args&&... args);

    /**
     * Finalizes the prepared statement.
     */
    ~SqliteStatement();

    /**
     * The various bind methods bind a value to the query parameter specified by paramIndex.
     *
     * @param paramIndex - zero-based index of a query parameter.
     */
    void bindInt(int paramIndex, int64_t intValue);

    void bindBlob(int paramIndex, const void* data, int len);

    void bindText(int paramIndex, const char* data, int len);

    void clearBindings();

    /**
     * Wraps sqlite3_step and returns the resulting status.
     *
     * @param desiredStatus - the desired return status of sqlite3_step. When desiredStatus is
     * non-negative, checkStatus compares desiredStatus with the returned status from sqlite3_step.
     * By default, checkStatus is ignored.
     */
    int step(int desiredStatus = -1);

    /**
     * The getCol methods wrap sqlite3_column methods and return the correctly typed values
     * stored in a retrieved query row[colIndex].
     *
     * @param colIndex - zero-based index of a column retrieved from a query row.
     */
    int64_t getColInt(int colIndex);

    const void* getColBlob(int colIndex);

    /**
     * Returns the number of bytes in a corresponding blob or string.
     */
    int64_t getColBytes(int colIndex);

    /**
     * Wraps sqlite3_column_text method and returns the text from the retrieved query row[colIndex].
     *
     * @param colIndex - zero-based index of a column retrieved from a query row.
     */
    const void* getColText(int colIndex);

    /**
     * Resets the statement to the first of the query result rows.
     */
    void reset();

    /**
     * Sets the last status on the prepared statement.
     */
    void setExceptionStatus(int status) {
        _exceptionStatus = status;
    }

    /**
     * A one step query execution that wraps sqlite3_prepare_v2(), sqlite3_step(), and
     * sqlite3_finalize().
     * None of the rows retrieved, if any, are saved before the query is finalized. Thus, this
     * method should not be used for read operations.
     */
    static void execQuery(MobileSession* session, const std::string& query);

    /**
     * Finalizes a prepared statement.
     */
    void finalize();

    /**
     * Prepare a statement with the given mobile session.
     */
    void prepare(const MobileSession& session);

    uint64_t _id;

private:
    static constexpr size_t MAX_FIXED_SIZE = 96;
    const char* getSqlQuery() const {
        return _sqlQuerySize > MAX_FIXED_SIZE ? _sqlQuery.dynamic : _sqlQuery.fixed;
    }

    static AtomicWord<long long> _nextID;
    sqlite3_stmt* _stmt;

    // If the most recent call to sqlite3_step on this statement returned an error, the error is
    // returned again when the statement is finalized. This is used to verify that the last error
    // code returned matches the finalize error code, if there is any.
    int _exceptionStatus = SQLITE_OK;

    std::size_t _sqlQuerySize;
    union {
        char fixed[MAX_FIXED_SIZE];
        const char* dynamic;
    } _sqlQuery;
};

namespace detail {
// Most of the strings we build statements with are static strings so we can calculate their length
// during compile time.
// Arrays decay to pointer in overload resolution, force that to not happen by providing true_type
// as second argument if array
template <std::size_t N>
constexpr std::size_t stringLength(char const (&)[N], std::true_type) {
    // Omit the null terminator, will added back when we call reserve later
    return N - 1;
}

inline std::size_t stringLength(const char* str, std::false_type) {
    return strlen(str);
}

inline std::size_t stringLength(const StringData& sd, std::false_type) {
    return sd.size();
}

inline std::size_t stringLength(const std::string& str, std::false_type) {
    return str.size();
}

template <std::size_t N>
constexpr void stringAppend(char*& dest, char const (&str)[N], std::true_type) {
    auto bytes = N - 1;
    memcpy(dest, str, bytes);
    dest = dest + bytes;
}

inline void stringAppend(char*& dest, const char* str, std::false_type) {
    dest = strcpy(dest, str) - 1;
}

inline void stringAppend(char*& dest, const StringData& sd, std::false_type) {
    memcpy(dest, sd.rawData(), sd.size());
    dest += sd.size();
}

inline void stringAppend(char*& dest, const std::string& str, std::false_type) {
    memcpy(dest, str.c_str(), str.size());
    dest += str.size();
}
}  // namespace detail

template <class... Args>
SqliteStatement::SqliteStatement(const MobileSession& session, Args&&... args) {
    // Increment the global instance count and assign this instance an id.
    _id = _nextID.addAndFetch(1);

    // Reserve the size we need once to avoid any additional allocations
    _sqlQuerySize = (detail::stringLength(std::forward<Args>(args),
                                          std::is_array<std::remove_reference_t<Args>>()) +
                     ...) +
        1;

    if (_sqlQuerySize > MAX_FIXED_SIZE) {
        _sqlQuery.dynamic = new char[_sqlQuerySize];
    }
    char* buffer = const_cast<char*>(getSqlQuery());

    (detail::stringAppend(
         buffer, std::forward<Args>(args), std::is_array<std::remove_reference_t<Args>>()),
     ...);
    *buffer = '\0';

    prepare(session);
}

}  // namespace mongo
