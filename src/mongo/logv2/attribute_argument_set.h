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
#include "mongo/logv2/formatting_ostream_iterator.h"
#include "mongo/logv2/visitor.h"
#include "mongo/stdx/variant.h"

namespace mongo {
namespace logv2 {

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
                  StringData,
                  std::function<void(FormattingVisitor*)>>
        value;

    constexpr arg_value(int val = 0) : value(val) {}
    constexpr arg_value(unsigned val) : value(val) {}
    constexpr arg_value(long long val) : value(val) {}
    constexpr arg_value(unsigned long long val) : value(val) {}
    constexpr arg_value(float val) : value(static_cast<double>(val)) {}
    constexpr arg_value(double val) : value(val) {}
    constexpr arg_value(long double val) : value(val) {}
    constexpr arg_value(bool val) : value(val) {}
    constexpr arg_value(char val) : value(val) {}
    arg_value(const char* val) : value(StringData(val)) {}
    arg_value(StringData val) : value(val) {}
    arg_value(std::string const& val) : value(StringData(val)) {}
    /*value(basic_string_view<char_type> val) {
        string = StringData(val.data(), val.size());
    }*/

    template <typename T>
    arg_value(const T& val) {
        using std::placeholders::_1;
        value.emplace<std::function<void(FormattingVisitor*)>>(std::bind(&T::format, val, _1));

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
        : data_{make_arg_value(args.value)...},
          name_{StringData(args.name.data(), args.name.size())...} {}
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

    void format(FormattingVisitor* visitor) const {
        const arg_value* data = data_;
        const StringData* name = name_;
        for (size_t i = 0; i < size_; ++i) {
            stdx::visit([visitor, name](auto&& arg) { visitor->write(*name, arg); }, data->value);

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
    // fmt::format_args _values;

    arg_erased_store _values2;

    fmt::basic_format_args<fmt::basic_format_context<formatting_ostream_iterator<>, char>> _values;
};


}  // namespace logv2
}  // namespace mongo
