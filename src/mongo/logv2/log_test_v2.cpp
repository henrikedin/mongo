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

#include <string>
#include <vector>

#include "mongo/bson/json.h"
#include "mongo/logv2/component_settings_filter.h"
#include "mongo/logv2/json_formatter.h"
#include "mongo/logv2/log.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/ramlog_sink.h"
#include "mongo/logv2/text_formatter.h"
#include "mongo/stdx/thread.h"

using namespace mongo::logv2;

namespace mongo {
namespace {
class LogTestBackend
    : public boost::log::sinks::
          basic_formatted_sink_backend<char, boost::log::sinks::synchronized_feeding> {
public:
    LogTestBackend(std::vector<std::string>& lines) : _logLines(lines) {}

    static boost::shared_ptr<boost::log::sinks::synchronous_sink<LogTestBackend>> create(
        std::vector<std::string>& lines) {
        auto backend = boost::make_shared<LogTestBackend>(lines);
        return boost::make_shared<boost::log::sinks::synchronous_sink<LogTestBackend>>(
            std::move(backend));
    }

    void consume(boost::log::record_view const& rec, string_type const& formatted_string) {
        _logLines.push_back(formatted_string);
    }

private:
    std::vector<std::string>& _logLines;
};

class PlainFormatter {
public:
    static bool binary() {
        return false;
    };

    void operator()(boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
        StringData message = boost::log::extract<StringData>(attributes::message(), rec).get();
        const auto& attrs =
            boost::log::extract<AttributeArgumentSet>(attributes::attributes(), rec).get();

        strm << fmt::internal::vformat(to_string_view(message), attrs.values);
    }
};

class LogDuringInitTester
{
public:
	LogDuringInitTester()
	{
        std::vector<std::string> lines;
        auto sink = LogTestBackend::create(lines);
        sink->set_filter(
            ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
        sink->set_formatter(PlainFormatter());
        LogManager::global().getGlobalDomain().impl().core()->add_sink(sink);

		LOGV2("log during init");
        ASSERT(lines.back() == "log during init");

		LogManager::global().getGlobalDomain().impl().core()->remove_sink(sink);
	}
};

LogDuringInitTester logDuringInit;

TEST_F(LogTestV2, logBasic) {
    std::vector<std::string> lines;
    auto sink = LogTestBackend::create(lines);
    sink->set_filter(ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    sink->set_formatter(PlainFormatter());
    attach(sink);

    LOGV2("test");
    ASSERT(lines.back() == "test");

    LOGV2("test {}", "name"_attr = 1);
    ASSERT(lines.back() == "test 1");

    LOGV2("test {:d}", "name"_attr = 2);
    ASSERT(lines.back() == "test 2");

    LOGV2("test {}", "name"_attr = "char*");
    ASSERT(lines.back() == "test char*");

    LOGV2("test {}", "name"_attr = std::string("std::string"));
    ASSERT(lines.back() == "test std::string");

    LOGV2("test {}", "name"_attr = "StringData"_sd);
    ASSERT(lines.back() == "test StringData");

    LOGV2_OPTIONS({LogTag::kStartupWarnings}, "test");
    ASSERT(lines.back() == "test");
}

TEST_F(LogTestV2, logText) {
    std::vector<std::string> lines;
    auto sink = LogTestBackend::create(lines);
    sink->set_filter(ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    sink->set_formatter(TextFormatter());
    attach(sink);

    LOGV2_OPTIONS({LogTag::kNone}, "warning");
    ASSERT(lines.back().rfind("** WARNING: warning") == std::string::npos);

    LOGV2_OPTIONS({LogTag::kStartupWarnings}, "warning");
    ASSERT(lines.back().rfind("** WARNING: warning") != std::string::npos);

    LOGV2_OPTIONS({static_cast<LogTag::Value>(LogTag::kStartupWarnings | LogTag::kJavascript)},
                  "warning");
    ASSERT(lines.back().rfind("** WARNING: warning") != std::string::npos);
}

TEST_F(LogTestV2, logJSON) {
    std::vector<std::string> lines;
    auto sink = LogTestBackend::create(lines);
    sink->set_filter(ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    sink->set_formatter(JsonFormatter());
    attach(sink);

    BSONObj log;

    LOGV2("test");
    log = mongo::fromjson(lines.back());
    ASSERT(log.getField("t"_sd).String() == dateToISOStringUTC(Date_t::lastNowForTest()));
    ASSERT(log.getField("s"_sd).String() == LogSeverity::Info().toStringDataCompact());
    ASSERT(log.getField("c"_sd).String() ==
           LogComponent(MONGO_LOGV2_DEFAULT_COMPONENT).getNameForLog());
    ASSERT(log.getField("ctx"_sd).String() == getThreadName());
    ASSERT(!log.hasField("id"_sd));
    ASSERT(log.getField("msg"_sd).String() == "test");
    ASSERT(!log.hasField("attr"_sd));
    ASSERT(!log.hasField("tags"_sd));

    LOGV2("test {}", "name"_attr = 1);
    log = mongo::fromjson(lines.back());
    ASSERT(log.getField("msg"_sd).String() == "test {name}");
    ASSERT(log.getField("attr"_sd).Obj().nFields() == 1);
    ASSERT(log.getField("attr"_sd).Obj().getField("name").Int() == 1);

    LOGV2("test {:d}", "name"_attr = 2);
    log = mongo::fromjson(lines.back());
    ASSERT(log.getField("msg"_sd).String() == "test {name:d}");
    ASSERT(log.getField("attr"_sd).Obj().nFields() == 1);
    ASSERT(log.getField("attr"_sd).Obj().getField("name").Int() == 2);

    LOGV2_OPTIONS({LogTag::kStartupWarnings}, "warning");
    log = mongo::fromjson(lines.back());
    ASSERT(log.getField("msg"_sd).String() == "warning");
    ASSERT(log.getField("tags"_sd).Int() == LogTag::kStartupWarnings);
}

TEST_F(LogTestV2, logThread) {
    std::vector<std::string> lines;
    auto sink = LogTestBackend::create(lines);
    sink->set_filter(ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    sink->set_formatter(PlainFormatter());
    attach(sink);

    constexpr int kNumPerThread = 100;
    std::vector<stdx::thread> threads;

    threads.emplace_back([&]() {
        for (int i = 0; i < kNumPerThread; ++i)
            LOGV2("thread1");
    });

    threads.emplace_back([&]() {
        for (int i = 0; i < kNumPerThread; ++i)
            LOGV2("thread2");
    });

    threads.emplace_back([&]() {
        for (int i = 0; i < kNumPerThread; ++i)
            LOGV2("thread3");
    });

    threads.emplace_back([&]() {
        for (int i = 0; i < kNumPerThread; ++i)
            LOGV2("thread4");
    });

    for (auto&& thread : threads) {
        thread.join();
    }

    ASSERT(lines.size() == threads.size() * kNumPerThread);
}

TEST_F(LogTestV2, logRamlog) {
    RamLog* ramlog = RamLog::get("test_ramlog");

    auto sink = RamLogSink::create(ramlog);
    sink->set_filter(ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    sink->set_formatter(PlainFormatter());
    attach(sink);

    std::vector<std::string> lines;
    auto testSink = LogTestBackend::create(lines);
    testSink->set_filter(
        ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    testSink->set_formatter(PlainFormatter());
    attach(testSink);

    auto verifyRamLog = [&]() {
        RamLog::LineIterator iter(ramlog);
        return std::all_of(lines.begin(), lines.end(), [&iter](const std::string& line) {
            return line == iter.next();
        });
    };

    LOGV2("test");
    ASSERT(verifyRamLog());
    LOGV2("test2");
    ASSERT(verifyRamLog());
}

TEST_F(LogTestV2, MultipleDomains) {
    std::vector<std::string> global_lines;
    auto sink = LogTestBackend::create(global_lines);
    sink->set_filter(ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    sink->set_formatter(PlainFormatter());
    attach(sink);

    class OtherDomainImpl : public LogDomainImpl {
    public:
        OtherDomainImpl() : _core(boost::log::core::create()) {}

        log_source& source() override {
            thread_local log_source lg(_core);
            return lg;
        }
        boost::shared_ptr<boost::log::core> core() override {
            return _core;
        }

    private:
        boost::shared_ptr<boost::log::core> _core;
    };

    LogDomain other_domain(std::make_unique<OtherDomainImpl>());
    std::vector<std::string> other_lines;
    auto other_sink = LogTestBackend::create(other_lines);
    other_sink->set_filter(
        ComponentSettingsFilter(LogManager::global().getGlobalDomain().settings()));
    other_sink->set_formatter(PlainFormatter());
    other_domain.impl().core()->add_sink(other_sink);


    LOGV2_OPTIONS({&other_domain}, "test");
    ASSERT(global_lines.empty());
    ASSERT(other_lines.back() == "test");

    LOGV2("global domain log");
    ASSERT(global_lines.back() == "global domain log");
    ASSERT(other_lines.back() == "test");
}

}  // namespace
}  // namespace mongo
