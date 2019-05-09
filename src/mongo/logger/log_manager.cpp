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

#include "mongo/platform/basic.h"

#include "mongo/logger/log_manager.h"

#include "mongo/logger/console_appender.h"
#include "mongo/logger/text_formatter.h"
#include "mongo/logger/json_formatter.h"
#include "mongo/logger/message_event_utf8_encoder.h"
#include "mongo/logger/severity_filter.h"
#include "mongo/logger/ramlog.h"
#include "mongo/logger/ramlog_sink.h"

#include <boost/log/core.hpp>
#include <boost/log/sinks.hpp>
#include <boost/core/null_deleter.hpp>

namespace mongo {
namespace logger {
typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend> console_sink_t;
boost::shared_ptr<console_sink_t> console_sink;
boost::shared_ptr<boost::log::sinks::unlocked_sink<RamLogSink>> startup_warnings_sink;

LogManager::LogManager() {
    reattachDefaultConsoleAppender();

	startup_warnings_sink =
        logger::create_ramlog_sink<logger::TextFormatter>(RamLog::get("startupWarnings"));
    startup_warnings_sink->set_filter([](boost::log::attribute_value_set const& attrs) {
        return boost::log::extract<LogDomain>(attributes::domain, attrs) == kStartupWarnings;
    });
    boost::log::core::get()->add_sink(startup_warnings_sink);
    
}

LogManager::~LogManager() {
    /*for (DomainsByNameMap::iterator iter = _domains.begin(); iter != _domains.end(); ++iter) {
        delete iter->second;
    }*/
}

//MessageLogDomain* LogManager::getNamedDomain(const std::string& name) {
//    MessageLogDomain*& domain = _domains[name];
//    if (!domain) {
//        domain = new MessageLogDomain;
//    }
//    return domain;
//}

void LogManager::detachDefaultConsoleAppender() {
    /*invariant(_defaultAppender);
    _globalDomain.detachAppender(_defaultAppender);
    _defaultAppender.reset();*/
    boost::log::core::get()->remove_sink(console_sink);
}

void LogManager::reattachDefaultConsoleAppender() {
    /*invariant(!_defaultAppender);
    _defaultAppender =
        _globalDomain.attachAppender(std::make_unique<ConsoleAppender<MessageEventEphemeral>>(
            std::make_unique<MessageEventDetailsEncoder>()));*/
    console_sink = boost::make_shared<console_sink_t>();

    console_sink->set_filter(SeverityFilter());
    console_sink->set_formatter(JsonFormatter());

    console_sink->locked_backend()->add_stream(
        boost::shared_ptr<std::ostream>(&std::cout, boost::null_deleter()));

    boost::log::core::get()->add_sink(console_sink);
}

bool LogManager::isDefaultConsoleAppenderAttached() const {
    //return static_cast<bool>(_defaultAppender);
    return console_sink.use_count() > 1;
}

void LogManager::writeLogBypassFilteringAndFormatting(StringData str) {
    boost::log::record_view view;
    console_sink->locked_backend()->consume(view, std::string(str));
}

}  // logger
}  // mongo
