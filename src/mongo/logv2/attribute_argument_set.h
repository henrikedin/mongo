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
//namespace {
//
//template <class T, class = void>
//struct has_toBSON : std::false_type {};
//
//template <class T>
//struct has_toBSON<T, std::void_t<decltype(std::declval<T>().toBSON())>> : std::true_type {};
//
//template <class T, class = void>
//struct has_toString : std::false_type {};
//
//template <class T>
//struct has_toString<T, std::void_t<decltype(std::declval<T>().toString())>> : std::true_type {};
//}  // namespace
//
//struct CustomAttributeValue {
//    std::function<BSONObj()> _toBSON;
//    std::function<std::string()> _toString;
//};
//
//class NamedAttribute {
//public:
//    StringData _name;
//    stdx::variant<int,
//                  unsigned,
//                  long long,
//                  unsigned long long,
//                  bool,
//                  char,
//                  double,
//                  StringData,
//                  CustomAttributeValue>
//        _value;
//
//    NamedAttribute() = default;
//    NamedAttribute(StringData name, int val = 0) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, unsigned val) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, long long val) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, unsigned long long val) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, float val) : _name(name), _value(static_cast<double>(val)) {}
//    NamedAttribute(StringData name, double val) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, bool val) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, char val) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, const char* val) : _name(name), _value(StringData(val)) {}
//    NamedAttribute(StringData name, StringData val) : _name(name), _value(val) {}
//    NamedAttribute(StringData name, std::string const& val) : _name(name), _value(StringData(val)) {}
//
//    template <typename T>
//    NamedAttribute(StringData name, const T& val) : _name(name) {
//        static_assert(has_toString<T>::value, "custom type need toString() implementation");
//
//        CustomAttributeValue custom;
//        if constexpr (has_toBSON<T>::value) {
//            custom._toBSON = std::bind(&T::toBSON, val);
//        }
//        if constexpr (has_toString<T>::value) {
//            custom._toString = std::bind(&T::toString, val);
//        }
//
//        _value = std::move(custom);
//    }
//};
//
//template <typename T>
//inline NamedAttribute makeNamedAttribute(StringData name, const T& val) {
//    return {name, val};
//}
//
//template <typename... Args>
//class AttributeStorage {
//private:
//    static const size_t num_args = sizeof...(Args);
//
//    NamedAttribute data_[num_args + (num_args == 0 ? 1 : 0)];
//
//    friend class TypeErasedAttributeStorage;
//
//public:
//    AttributeStorage(const Args&... args)
//        : data_{makeNamedAttribute(StringData(args.name.data(), args.name.size()), args.value)...} {}
//};
//
//template <typename... Args>
//inline AttributeStorage<Args...> makeAttributeStorage(const Args&... args) {
//    return {args...};
//}
//
//class TypeErasedAttributeStorage {
//public:
//    TypeErasedAttributeStorage() : size_(0) {}
//
//    template <typename... Args>
//    TypeErasedAttributeStorage(const AttributeStorage<Args...>& store) {
//        data_ = store.data_;
//        size_ = store.num_args;
//    }
//
//    void format(FormattingVisitor* visitor) const {
//        /*const arg_value* data = data_;
//        const StringData* name = name_;
//        for (size_t i = 0; i < size_; ++i) {
//            stdx::visit([visitor, name](auto&& arg) { visitor->write(*name, arg); }, data->value);
//
//            data++;
//            name++;
//        }*/
//    }
//
//private:
//    const NamedAttribute* data_;
//    size_t size_;
//};

// Type erased set of provided libfmt named arguments. Index match between names and values.
struct AttributeArgumentSet {
    boost::container::small_vector<StringData, fmt::internal::max_packed_args> _names;
    // fmt::format_args _values;

    //TypeErasedAttributeStorage _values2;

    fmt::basic_format_args<fmt::basic_format_context<formatting_ostream_iterator<>, char>> _values;
};


}  // namespace logv2
}  // namespace mongo
