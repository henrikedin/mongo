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
#include "mongo/logger/message_event_utf8_encoder.h"

#include <boost/log/core.hpp>
#include <boost/log/sinks.hpp>
#include <boost/core/null_deleter.hpp>

namespace mongo {
namespace logger {

LogManager::LogManager() {
    reattachDefaultConsoleAppender();

	typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend>
        text_sink_t;
    auto text_sink = boost::make_shared<text_sink_t>();

    //text_sink->set_formatter(
    //    [](boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
    //        strm << boost::log::extract<std::string>("TimeStamp", rec)
    //             << " "
    //             //<< boost::log::expressions::wrap_formatter(&severity_formatter) << " "
    //             << boost::log::extract<std::string>("Channel", rec) << " ["
    //             << boost::log::extract<std::string>("ThreadName", rec) << "] "
    //             << rec[boost::log::expressions::smessage];
    //    });

    text_sink->locked_backend()->add_stream(
        boost::shared_ptr<std::ostream>(&std::cout, boost::null_deleter()));

    boost::log::core::get()->add_sink(std::move(text_sink));
}

LogManager::~LogManager() {
    for (DomainsByNameMap::iterator iter = _domains.begin(); iter != _domains.end(); ++iter) {
        delete iter->second;
    }
}

MessageLogDomain* LogManager::getNamedDomain(const std::string& name) {
    MessageLogDomain*& domain = _domains[name];
    if (!domain) {
        domain = new MessageLogDomain;
    }
    return domain;
}

void LogManager::detachDefaultConsoleAppender() {
    invariant(_defaultAppender);
    _globalDomain.detachAppender(_defaultAppender);
    _defaultAppender.reset();
}

void LogManager::reattachDefaultConsoleAppender() {
    invariant(!_defaultAppender);
    _defaultAppender =
        _globalDomain.attachAppender(std::make_unique<ConsoleAppender<MessageEventEphemeral>>(
            std::make_unique<MessageEventDetailsEncoder>()));
}

bool LogManager::isDefaultConsoleAppenderAttached() const {
    return static_cast<bool>(_defaultAppender);
}

}  // logger
}  // mongo
