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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/logv2/log_test_v2.h"

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "mongo/logv2/log.h"
#include "mongo/logv2/log_component.h"
//#include "mongo/logv2/log_component_settings.h"
//#include "mongo/platform/compiler.h"
//#include "mongo/unittest/unittest.h"
//#include "mongo/util/concurrency/thread_name.h"
//#include "mongo/util/log.h"
//#include "mongo/util/str.h"

// struct json_visitor {
//    template <typename T>
//    void visit_object(T& obj);
//
//    template <typename T>
//    void visit(T& obj) {
//        visit_object(obj);
//    }
//
//    template <>
//    void visit<double>(double& obj) {
//        ss << obj;
//    }
//
//	template <>
//    void visit<mongo::StringData>(mongo::StringData& obj) {
//        ss << obj.toString();
//    }
//
//    template <typename T>
//    void visit_member(const char* name, T& member) {
//        if (!first_member)
//            ss << ',';
//        ss << '"' << name << "\":";
//        visit(member);
//        first_member = false;
//    }
//
//    std::string to_string() {
//        return ss.str();
//    }
//
//    std::stringstream ss;
//    bool first_member = true;
//};
//
// template <typename T>
// void json_visitor::visit_object(T& obj) {
//    ss << "{";
//    first_member = true;
//    obj.visit_members(*this);
//    ss << "}";
//}
//
// struct point {
//    double x, y;
//    point() : x(0.0), y(0.0) {}
//    point(double _x, double _y) : x(_x), y(_y) {}
//
//    template <typename Visitor>
//    void visit_members(Visitor& v) {
//        v.visit_member("fmt", mongo::StringData("({:.1f}, {:.1f})"));
//        v.visit_member("x", x);
//        v.visit_member("y", y);
//    }
//};
//
// struct triangle {
//    point p1{1.0, 2.0};
//    point p2{3.0, 4.0};
//    point p3{5.0, 6.0};
//
//    template <typename Visitor>
//    void visit_members(Visitor& v) {
//        v.visit_member("fmt", mongo::StringData ("{} {} {}"));
//        v.visit_member("p1", p1);
//        v.visit_member("p2", p2);
//        v.visit_member("p3", p3);
//    }
//};
//
// namespace fmt {
// template <>
// struct formatter<point> {
//    template <typename ParseContext>
//    constexpr auto parse(ParseContext& ctx) {
//        return ctx.begin();
//    }
//
//    template <typename FormatContext>
//    auto format(const point& p, FormatContext& ctx) {
//        return format_to(ctx.out(), "({:.1f}, {:.1f})", p.x, p.y);
//    }
//};
//}  // namespace fmt

using namespace mongo::logv2;

namespace mongo {
namespace {


// template <typename S, typename Tuple, std::size_t... I>
// void performLog(S const& s, Tuple&& t, std::index_sequence<I...>) {
//    auto str = fmt::format(s, std::get<I>(t)...);
//    int i = 5;
//}
//
// template <typename S, typename... Args>
// void doLog(S const& s, Args&&... args) {
//    auto saved_args = std::make_tuple((args.value)...);
//    performLog(s, saved_args, std::index_sequence_for<Args...>{});
//}

template <typename S, typename... Args>
void doLog(S const& s, fmt::internal::named_arg<Args, char>&&... args) {
    auto str = fmt::format(s, (args.value)...);
    int i = 5;
    // std::apply(fmt::format, saved_args);
    // auto str = fmt::format();
}

#define DOTHELOG(MESSAGE, ...) doLog(FMT_STRING(MESSAGE), __VA_ARGS__)

#define _GET_NTH_ARG(_1, _2, _3, _4, _5, N, ...) N

#define _fe_0(_call, ...)

#define _fe_1(_call, x) _call(x)

#define _fe_2(_call, x, ...) _call(x) _fe_1(_call, __VA_ARGS__)

#define _fe_3(_call, x, ...) _call(x) _fe_2(_call, __VA_ARGS__)

#define _fe_4(_call, x, ...) _call(x) _fe_3(_call, __VA_ARGS__)


#define CALL_MACRO_X_FOR_EACH(x, ...) \
    _GET_NTH_ARG("ignored", ##__VA_ARGS__, _fe_4, _fe_3, _fe_2, _fe_1, _fe_0)(x, ##__VA_ARGS__)

#define CONSTRUCT_NAMED_ARG(x) make_named##x
#define DOTHELOG2(MESSAGE, ...) \
    doLog(FMT_STRING(MESSAGE), CALL_MACRO_X_FOR_EACH(CONSTRUCT_NAMED_ARG, __VA_ARGS__))

template <typename T>
fmt::internal::named_arg<T, char> make_named(const char* n, T&& val) {
    return fmt::internal::named_arg<T, char>(n, std::forward<T>(val));
}

TEST_F(LogTestV2, logContext) {
    using namespace fmt::literals;
    /*log_new("test");
    log_new({LogComponent{LogComponent::kControl}}, "test");
    log_new(FMT_STRING("test {} {}"), "asd", 3);
    log_new("test {} {}"_log, "asd", 3);*/
    // 2_blah;

    /*point p;
    p.x = 1.0;
    p.y = 2.0;
    triangle t;*/

    // json_visitor v;
    // v.visit(t);
    // auto str = v.to_string();
    // std::cout << str << std::endl;


    // DOTHELOG("test {0:d} {1}", "asd"_a = 3, "sfd"_a = "sfd", "dfg"_a = 46);
    LOGV2_WARNING("test {1} {0}", "asd"_a = 3, "sfd"_a = "sfd2", "dfg"_a = 46);
    LOGV2_STABLE("thestableid"_sd, "test {0} {1}", "asd"_a = 3, "sfd"_a = "sfd2", "dfg"_a = 46);
    LOGV2_OPTIONS({LogTag::kStartupWarnings},
                  "test {0} {1}",
                  "asd"_a = 3,
                  "sfd"_a = "sfd",
                  "dfg"_a = 46);

	LOGV2_OPTIONS({LogComponent::kStorageRecovery},
                  "test {0} {1}",
                  "asd"_a = 3,
                  "sfd"_a = "sfd",
                  "dfg"_a = 46);
	

    LOGV2_DEBUG1("test {:d} {}", "asd"_a = 3, "sfd"_a = "sfd", "dfg"_a = 46);

    // doLog(FMT_STRING("test {:d} {}"), {"asd", 3}, {"sfd", 7});
    // DOTHELOG2("test {:d} {}", {"asd", 3}, {"sfd","sdf"});
}

}  // namespace
}  // namespace mongo
