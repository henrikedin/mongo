/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#include <tuple>
#include <type_traits>
#include <fmt/format.h>

namespace mongo {
namespace logv2 {
namespace detail {
// Helper to be used inside attr() functions, captures rvalues by value so they don't go out of
// scope Can create a tuple of loggable named attributes for the logger
template <typename... T>
class ComposedAttr {
public:
    ComposedAttr(T&&... args) : _values(std::forward<T>(args)...) {}

    // Creates a flattend tuple of loggable named attributes
    auto attributes() const;

private:
    std::tuple<T...> _values;
};

template <class T>
struct IsComposedAttr : std::false_type {};

template <class... T>
struct IsComposedAttr<ComposedAttr<T...>> : std::true_type {};

// Helper to make regular attributes composable with cat()
template <class T>
auto attr(const fmt::internal::named_arg<T, char>& a) {
    return a;
}

// Flattens the input into a single tuple (no tuples of tuples). Passes through the input by
// reference by possible. May only be used at the call side of the log system to avoid dangling
// references.
template <typename T>
decltype(auto) flattenRef(const T& arg) {
    if constexpr (IsComposedAttr<T>::value)
        return arg.attributes();
    else
        return std::make_tuple(std::ref(arg));
}

// Same as above but does not pass by reference. Needs to be used when building composed hierarchies
// in helper functions
template <typename T>
decltype(auto) flatten(const T& arg) {
    if constexpr (IsComposedAttr<T>::value)
        return arg.attributes();
    else
        return std::make_tuple(arg);
}

template <typename... T>
auto ComposedAttr<T...>::attributes() const {
    // attr() converts the input to a loggable named attribute, user implementation
    return std::apply([this](auto&&... args) { return std::tuple_cat(flatten(attr(args))...); },
                      _values);
}

}  // namespace detail

// Concatenates multiple attributes to be returned in user defined attr() functions
template <class... T>
auto cat(T&&... args) {
    return detail::ComposedAttr<
        std::conditional_t<std::is_lvalue_reference_v<T>, T, std::remove_reference_t<T>>...>(
        std::forward<T>(args)...);
}

}  // namespace logv2

inline namespace literals {
inline fmt::internal::udl_arg<char> operator"" _attr(const char* s, std::size_t) {
    return {s};
}
}  // namespace literals
}  // namespace mongo
