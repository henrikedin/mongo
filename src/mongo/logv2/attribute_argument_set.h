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

#include <functional>

#include <boost/container/small_vector.hpp>
#include <fmt/format.h>

#include "mongo/base/string_data.h"
#include "mongo/stdx/variant.h"

namespace mongo {
namespace logv2 {

class visitor_base {
public:
    void write(StringData name, int val) {
        write_int(name, val);
	}
    void write(StringData name, unsigned val) {
        write_unsigned(name, val);
    }
    void write(StringData name, long long val) {
        write_longlong(name, val);
    }
    void write(StringData name, unsigned long long val) {
        write_unsigned_longlong(name, val);
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
    void write(StringData name, const void* val) {
        write_pointer(name, val);
    }
    void write(StringData name, StringData val) {
        write_string(name, val);
    }
    
	virtual void write_int(StringData name, int val) = 0;
    virtual void write_unsigned(StringData name, unsigned val) = 0;
    virtual void write_longlong(StringData name, long long val) = 0;
    virtual void write_unsigned_longlong(StringData name, unsigned long long val) = 0;
    virtual void write_bool(StringData name, bool val) = 0;
    virtual void write_char(StringData name, char val) = 0;
    virtual void write_double(StringData name, double val) = 0;
    virtual void write_long_double(StringData name, long double val) = 0;
    virtual void write_pointer(StringData name, const void* val) = 0;
    virtual void write_string(StringData name, StringData val) = 0;
    
	virtual void begin_member(StringData name) = 0;
    virtual void end_member() = 0;
};

class arg_value {
public:
    // union {
    //    int int_value;
    //    unsigned uint_value;
    //    long long long_long_value;
    //    unsigned long long ulong_long_value;
    //    bool bool_value;
    //    char char_value;
    //    double double_value;
    //    long double long_double_value;
    //    const void* pointer;
    //    StringData string;
    //    custom_value<Context> custom;
    //    //const named_arg_base<char_type>* named_arg;
    //};
    stdx::variant<int,
                  unsigned,
                  long long,
                  unsigned long long,
                  bool,
                  char,
                  double,
                  long double,
                  const void*,
                  StringData,
                  std::function<void(visitor_base*)>>
        value;
    size_t index;

    constexpr arg_value(int val = 0) : value(val), index(0) {}
    constexpr arg_value(unsigned val) : value(val), index(1) {}
    constexpr arg_value(long long val) : value(val), index(2) {}
    constexpr arg_value(unsigned long long val) : value(val), index(3) {}
    constexpr arg_value(double val) : value(val), index(6) {}
    constexpr arg_value(long double val) : value(val), index(7) {}
    constexpr arg_value(bool val) : value(val), index(4) {}
    constexpr arg_value(char val) : value(val), index(5) {}
    arg_value(const char* val) : value(StringData(val)), index(9) {}
    arg_value(StringData val) : value(val), index(9) {}
    /*value(basic_string_view<char_type> val) {
        string = StringData(val.data(), val.size());
    }*/
    constexpr arg_value(const void* val) : value(val), index(8) {}

    template <typename T>
    arg_value(const T& val) {
        using std::placeholders::_1;
        value.emplace<std::function<void(visitor_base*)>>(std::bind(&T::format, val, _1));
        index = 10;
        // stdx::get<10>(value)(nullptr);
        // custom.value = &val;
        //// Get the formatter type through the context to allow different contexts
        //// have different extension points, e.g. `formatter<T>` for `format` and
        //// `printf_formatter<T>` for `printf`.
        // custom.format =
        //    format_custom_arg<T,
        //                      conditional_t<has_formatter<T, Context>::value,
        //                                    typename Context::template formatter_type<T>,
        //                                    fallback_formatter<T, char_type>>>;
    }

    // value(const named_arg_base<char_type>& val) {
    //    named_arg = &val;
    //}

private:
    // Formats an argument of a custom type, such as a user-defined class.
    /*template <typename T, typename Formatter>
    static void format_custom_arg(const void* arg,
                                  basic_parse_context<char_type>& parse_ctx,
                                  Context& ctx) {
        Formatter f;
        parse_ctx.advance_to(f.parse(parse_ctx));
        ctx.advance_to(f.format(*static_cast<const T*>(arg), ctx));
    }*/
};

template <typename T>
inline arg_value make_arg_value(const T& val) {
    return val;
}

/**
  \rst
  An array of references to arguments. It can be implicitly converted into
  `~fmt::basic_format_args` for passing into type-erased formatting functions
  such as `~fmt::vformat`.
  \endrst
 */
template <typename... Args>
class arg_store {
private:
    static const size_t num_args = sizeof...(Args);
    static const bool is_packed = true;
    // static const bool is_packed = num_args < internal::max_packed_args;

    // using value_type =
    //    conditional_t<is_packed, internal::value<Context>, basic_format_arg<Context>>;
    // using value_type = arg_value;

    // If the arguments are not packed, add one more element to mark the end.
    arg_value data_[num_args + (!is_packed || num_args == 0 ? 1 : 0)];
    StringData name_[num_args + (!is_packed || num_args == 0 ? 1 : 0)];

    friend class arg_erased_store;

public:
    /*static constexpr unsigned long long types = is_packed
        ? internal::encode_types<Context, Args...>()
        : internal::is_unpacked_bit | num_args;
    FMT_DEPRECATED static constexpr unsigned long long TYPES = types;*/

    arg_store(const Args&... args)
        : data_{make_arg_value(args.value)...}, name_{StringData(args.name.data(), args.name.size())...} {}
};

template <typename... Args>
inline arg_store<Args...> make_arg_store(const Args&... args) {
    return {args...};
}

class arg_erased_store {
public:
    arg_erased_store() : size_(0) {}

    template <typename... Args>
    arg_erased_store(const arg_store<Args...>& store) {
        data_ = store.data_;
        name_ = store.name_;
        size_ = store.num_args;
    }

    void format(visitor_base* visitor) const {
        const arg_value* data = data_;
        const StringData* name = name_;
        for (size_t i = 0; i < size_; ++i) {
            switch (data->index) {
                case 0:
                    visitor->write(*name, stdx::get<0>(data->value));
                    break;
                case 1:
                    visitor->write(*name, stdx::get<1>(data->value));
                    break;
                case 2:
                    visitor->write(*name, stdx::get<2>(data->value));
                    break;
                case 3:
                    visitor->write(*name, stdx::get<3>(data->value));
                    break;
                case 4:
                    visitor->write(*name, stdx::get<4>(data->value));
                    break;
                case 5:
                    visitor->write(*name, stdx::get<5>(data->value));
                    break;
                case 6:
                    visitor->write(*name, stdx::get<6>(data->value));
                    break;
                case 7:
                    visitor->write(*name, stdx::get<7>(data->value));
                    break;
                case 8:
                    visitor->write(*name, stdx::get<8>(data->value));
                    break;
                case 9:
                    visitor->write(*name, stdx::get<9>(data->value));
                    break;
                case 10:
                    visitor->begin_member(*name);
                    stdx::get<10>(data->value)(visitor);
                    visitor->end_member();
                    break;
            };

            data++;
            name++;
        }
    }

private:
    const arg_value* data_;
    const StringData* name_;
    size_t size_;
};

// Type erased set of provided libfmt named arguments. Index match between names and values.
struct AttributeArgumentSet {
    boost::container::small_vector<StringData, fmt::internal::max_packed_args> _names;
    fmt::format_args _values;

    arg_erased_store _values2;
};


}  // namespace logv2
}  // namespace mongo
