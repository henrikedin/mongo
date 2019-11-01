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

#include "mongo/base/string_data.h"

namespace mongo {

class FormattingVisitor {
public:
    void write(StringData name, int val) {
        write_int32(name, static_cast<int32_t>(val));
    }
    void write(StringData name, unsigned val) {
        write_uint32(name, static_cast<uint32_t>(val));
    }
    void write(StringData name, long long val) {
        write_int64(name, static_cast<int64_t>(val));
    }
    void write(StringData name, unsigned long long val) {
        write_uint64(name, static_cast<uint64_t>(val));
    }
    void write(StringData name, bool val) {
        write_bool(name, val);
    }
    void write(StringData name, char val) {
        write_char(name, val);
    }
    void write(StringData name, double val) {
        write_double(name, val);
    }
    void write(StringData name, long double val) {
        write_long_double(name, val);
    }
    void write(StringData name, StringData val) {
        write_string(name, val);
    }
    void write(StringData name, const std::function<void(FormattingVisitor*)>& custom_obj) {
        write_name(name);
        custom_obj(this);
    }

    template <typename T>
	void write(StringData name, const T& obj) {
        write_name(name);
        obj->format(this);
	}

    virtual void write_bool(StringData name, bool val) = 0;
    virtual void write_char(StringData name, char val) = 0;
    virtual void write_int32(StringData name, int32_t val) = 0;
    virtual void write_uint32(StringData name, uint32_t val) = 0;
    virtual void write_int64(StringData name, int64_t val) = 0;
    virtual void write_uint64(StringData name, uint64_t val) = 0;

    virtual void write_double(StringData name, double val) = 0;
    virtual void write_long_double(StringData name, long double val) = 0;
    virtual void write_string(StringData name, StringData val) = 0;

    virtual void write_name(StringData name) = 0;

    virtual void object_begin() = 0;
    virtual void object_end() = 0;
    virtual void array_begin() = 0;
    virtual void array_end() = 0;
};
}  // namespace mongo