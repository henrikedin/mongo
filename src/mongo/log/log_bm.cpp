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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::log::LogComponent::kDefault

#include "mongo/log/component_settings_filter.h"
#include "mongo/log/log.h"
#include "mongo/log/log_domain_global.h"
#include "mongo/log/text_formatter.h"
#include "mongo/platform/basic.h"

#include <benchmark/benchmark.h>
#include <boost/iostreams/device/null.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/make_shared.hpp>
#include <iostream>


namespace mongo {
namespace {

boost::shared_ptr<std::ostream> makeNullStream() {
    namespace bios = boost::iostreams;
    return boost::make_shared<bios::stream<bios::null_sink>>(bios::null_sink{});
}

// RAII style helper class for init/deinit new log system
class ScopedlogBench {
public:
    ScopedlogBench(benchmark::State& state) {
        _shouldInit = state.thread_index == 0;
        if (_shouldInit) {
            setupAppender();
        }
    }

    ~ScopedlogBench() {
        if (_shouldInit) {
            tearDownAppender();
        }
    }

private:
    void setupAppender() {
        log::LogDomainGlobal::ConfigurationOptions config;
        config.makeDisabled();
        invariant(log::LogManager::global().getGlobalDomainInternal().configure(config).isOK());

        auto backend = boost::make_shared<boost::log::sinks::text_ostream_backend>();
        backend->add_stream(makeNullStream());
        backend->auto_flush(true);

        _sink = boost::make_shared<
            boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend>>(backend);
        _sink->set_filter(
            log::ComponentSettingsFilter(log::LogManager::global().getGlobalDomain(),
                                         log::LogManager::global().getGlobalSettings()));
        _sink->set_formatter(log::TextFormatter());
        boost::log::core::get()->add_sink(_sink);
    }

    void tearDownAppender() {
        boost::log::core::get()->remove_sink(_sink);
        invariant(log::LogManager::global().getGlobalDomainInternal().configure({}).isOK());
    }

    boost::shared_ptr<boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend>>
        _sink;
    bool _shouldInit;
};

// "Expensive" way to create a string.
std::string createLongString() {
    return std::string(1000, 'a') + std::string(1000, 'b') + std::string(1000, 'c') +
        std::string(1000, 'd') + std::string(1000, 'e');
}

void BM_Nooplog(benchmark::State& state) {
    ScopedlogBench init(state);

    for (auto _ : state)
        LOG_DEBUG(20074, 1, "noop log");
}

void BM_NooplogArg(benchmark::State& state) {
    ScopedlogBench init(state);

    for (auto _ : state)
        LOG_DEBUG(20075, 1, "noop log {}", "str"_attr = createLongString());
}

void BM_EnabledLog(benchmark::State& state) {
    ScopedlogBench init(state);

    for (auto _ : state)
        LOG(20071, "enabled log");
}

void BM_EnabledLogExpensiveArg(benchmark::State& state) {
    ScopedlogBench init(state);

    for (auto _ : state)
        LOG(20072, "enabled log {}", "str"_attr = createLongString());
}

void BM_EnabledLogManySmallArg(benchmark::State& state) {
    ScopedlogBench init(state);

    for (auto _ : state) {
        LOG(20073,
            "enabled log {}{}{}{}{}{}{}{}{}{}",
            "1"_attr = 1,
            "2"_attr = 2,
            "3"_attr = "3",
            "4"_attr = 4.0,
            "5"_attr = "5",
            "6"_attr = "6"_sd,
            "7"_attr = 7,
            "8"_attr = 8,
            "9"_attr = "9",
            "10"_attr = "10"_sd);
    }
}

void ThreadCounts(benchmark::internal::Benchmark* b) {
    int tc[] = {1, 2, 4, 8};
    for (int t : tc)
        b->Threads(t);
}

BENCHMARK(BM_Nooplog)->Apply(ThreadCounts);
BENCHMARK(BM_NooplogArg)->Apply(ThreadCounts);
BENCHMARK(BM_EnabledLog)->Apply(ThreadCounts);
BENCHMARK(BM_EnabledLogExpensiveArg)->Apply(ThreadCounts);
BENCHMARK(BM_EnabledLogManySmallArg)->Apply(ThreadCounts);

}  // namespace
}  // namespace mongo
