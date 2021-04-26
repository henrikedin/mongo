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

#include "mongo/platform/basic.h"

#include "mongo/unittest/unittest.h"
#include "mongo/db/timeseries/minmax.h"

#include <numeric>

namespace mongo {
namespace {

std::string concatFieldNames(const MinMaxObj& obj) {
    return std::accumulate(
        obj.begin(), obj.end(), std::string(), [](std::string accum, const MinMaxElement& elem) {
            return std::move(accum) + elem.fieldName();
        });
}

TEST(MinMax, Simple) {
    MinMaxStore minmax;

    auto obj = minmax.root();
    ASSERT_EQ(std::distance(obj.begin(), obj.end()), 0);

    {
        auto [inserted, end] = obj.insert(obj.begin(), "b");
        ASSERT(obj.begin() == inserted);
        ASSERT(obj.end() == end);
        ASSERT_EQ(std::distance(inserted, end), 1);
        ASSERT_EQ(inserted->fieldName(), "b");
    }

    {
        auto [inserted, end] = obj.insert(obj.begin(), "a");
        ASSERT(obj.begin() == inserted);
        ASSERT(obj.end() == end);
        ASSERT_EQ(std::distance(inserted, end), 2);
        ASSERT_EQ(inserted->fieldName(), "a");
        ++inserted;
        ASSERT_EQ(inserted->fieldName(), "b");
        ASSERT_EQ(concatFieldNames(obj), "ab");
    }

    {
        auto [inserted, end] = obj.insert(obj.end(), "d");
        ASSERT(obj.end() == end);
        ASSERT_EQ(std::distance(inserted, end), 1);
        ASSERT_EQ(inserted->fieldName(), "d");
        ASSERT_EQ(concatFieldNames(obj), "abd");
    }

    { 
        auto it = obj.begin();
        ++it;
        ++it;
        auto [inserted, end] = obj.insert(it, "c");
        ASSERT_EQ(concatFieldNames(obj), "abcd");
    }
}

TEST(MinMax, SubObj) {
    MinMaxStore minmax;
    auto obj = minmax.root();
    auto [inserted, _] = obj.insert(obj.end(), "a");
    
    auto subobj = obj.object(inserted);
    ASSERT_EQ(std::distance(subobj.begin(), subobj.end()), 0);
    ASSERT(obj.begin() != subobj.begin());
    ASSERT(obj.end() == subobj.end());
    ASSERT(obj.begin() == subobj.parent().begin());

    { 
        subobj.insert(subobj.begin(), "b");
        subobj.insert(subobj.end(), "c");
        obj = subobj.parent();
    }

    ASSERT_EQ(concatFieldNames(obj) + concatFieldNames(obj.object(obj.begin())), "abc");

    obj.insert(obj.end(), "d");
    ASSERT_EQ(concatFieldNames(obj) + concatFieldNames(obj.object(obj.begin())), "adbc");

    std::tie(inserted, _) = obj.insert(obj.begin(), "x");
    ++inserted;
    ASSERT_EQ(concatFieldNames(obj) + concatFieldNames(obj.object(inserted)), "xadbc");


}


}  // namespace
}  // namespace mongo
