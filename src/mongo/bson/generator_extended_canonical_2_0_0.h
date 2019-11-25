/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include "mongo/bson/bsonobj.h"
#include "mongo/platform/decimal128.h"
#include "mongo/util/base64.h"
#include "mongo/util/str_escape.h"

#include <fmt/format.h>

namespace mongo {
class ExtendedCanonicalV200Generator {
public:
    void writeNull(fmt::memory_buffer& buffer) const {
        fmt::format_to(buffer, "null");
    }
    void writeUndefined(fmt::memory_buffer& buffer) const {
        fmt::format_to(buffer, R"({{"$undefined":true}})");
    }

    void writeString(fmt::memory_buffer& buffer, StringData str) const {
        buffer.push_back('"');
        str::escapeForJSON(buffer, str);
        buffer.push_back('"');
    }

    void writeBool(fmt::memory_buffer& buffer, bool val) const {
        fmt::format_to(buffer, val ? "true" : "false");
    }

    void writeInt32(fmt::memory_buffer& buffer, int32_t val) const {
        fmt::format_to(buffer, R"({{"$numberInt":"{}"}})", val);
    }

    void writeInt64(fmt::memory_buffer& buffer, int64_t val) const {
        fmt::format_to(buffer, R"({{"$numberLong":"{}"}})", val);
    }

    void writeDouble(fmt::memory_buffer& buffer, double val) const {
        if (val >= std::numeric_limits<double>::lowest() &&
            val <= std::numeric_limits<double>::max())
            fmt::format_to(buffer, R"({{"$numberDouble":"{}"}})", val);
        else if (std::isnan(val))
            fmt::format_to(buffer, R"({{"$numberDouble":"NaN"}})");
        else if (std::isinf(val))
            fmt::format_to(
                buffer, R"({{"$numberDouble":"{}"}})", val > 0 ? "Infinity"_sd : "-Infinity"_sd);
        else {
            StringBuilder ss;
            ss << "Number " << val << " cannot be represented in JSON";
            uassert(51744, ss.str(), false);
        }
    }

    void writeDecimal128(fmt::memory_buffer& buffer, Decimal128 val) const {
        if (val.isNaN())
            fmt::format_to(buffer, R"({{"$numberDecimal":"NaN"}})");
        else if (val.isInfinite())
            fmt::format_to(buffer,
                           R"({{"$numberDecimal":"{}"}})",
                           val.isNegative() ? "-Infinity"_sd : "Infinity"_sd);
        else {
            fmt::format_to(buffer, R"({{"$numberDecimal":"{}"}})", val.toString());
        }
    }

    void writeDate(fmt::memory_buffer& buffer, Date_t val) const {
        fmt::format_to(buffer, R"({{"$date":{{"$numberLong":"{}"}}}})", val.toMillisSinceEpoch());
    }

    void writeDBRef(fmt::memory_buffer& buffer, StringData ref, OID id) const {
        // Collection names can unfortunately contain control characters that need to be escaped
        fmt::format_to(buffer, R"({{"$ref":")");
        str::escapeForJSON(buffer, ref);

        // OID is a hex string and does not need to be escaped
        fmt::format_to(buffer, R"(","$id":"{}"}})", id.toString());
    }

    void writeOID(fmt::memory_buffer& buffer, OID val) const {
        // OID is a hex string and does not need to be escaped
        fmt::format_to(buffer, R"({{"$oid":"{}"}})", val.toString());
    }

    void writeTimestamp(fmt::memory_buffer& buffer, Timestamp val) const {
        fmt::format_to(
            buffer, R"({{"$timestamp":{{"t":{},"i":{}}}}})", val.getSecs(), val.getInc());
    }

    void writeBinData(fmt::memory_buffer& buffer,
                      const char* data,
                      int32_t size,
                      BinDataType type) const {
        fmt::format_to(buffer, R"({{"$binary":{{"base64":")");
        base64::encode(buffer, data, size);
        fmt::format_to(buffer, R"(","subType":"{:x}"}}}})", type);
    }

    void writeRegex(fmt::memory_buffer& buffer, StringData pattern, StringData options) const {
        fmt::format_to(buffer, R"({{"$regularExpression":{{"pattern":")");
        str::escapeForJSON(buffer, pattern);
        fmt::format_to(buffer, R"(","options":")");
        str::escapeForJSON(buffer, options);
        fmt::format_to(buffer, R"("}}}})");
    }

    void writeSymbol(fmt::memory_buffer& buffer, StringData symbol) const {
        fmt::format_to(buffer, R"({{"$symbol":")");
        str::escapeForJSON(buffer, symbol);
        fmt::format_to(buffer, R"("}})");
    }

    void writeCode(fmt::memory_buffer& buffer, StringData code) const {
        fmt::format_to(buffer, R"({{"$code":")");
        str::escapeForJSON(buffer, code);
        fmt::format_to(buffer, R"("}})");
    }
    void writeCodeWithScope(fmt::memory_buffer& buffer,
                            StringData code,
                            BSONObj const& scope) const {
        fmt::format_to(buffer, R"({{"$code":")");
        str::escapeForJSON(buffer, code);
        fmt::format_to(buffer, R"(","$scope":)");
        scope.jsonStringGenerator(*this, 0, false, buffer);
        fmt::format_to(buffer, R"(}})");
    }
    void writeMinKey(fmt::memory_buffer& buffer) const {
        fmt::format_to(buffer, R"({{"$minKey":1}})");
    }
    void writeMaxKey(fmt::memory_buffer& buffer) const {
        fmt::format_to(buffer, R"({{"$maxKey":1}})");
    }
    void writePadding(fmt::memory_buffer& buffer) const {}
};
}  // namespace mongo
