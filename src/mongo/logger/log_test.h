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

#pragma once

#include <sstream>
#include <string>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/logger/appender.h"
#include "mongo/logger/log_severity.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/message_log_domain.h"
#include "mongo/logger/severity_filter.h"
#include "mongo/unittest/unittest.h"

#include <boost/log/core.hpp>
#include <boost/log/sinks.hpp>

namespace mongo {
namespace logger {

// Used for testing logging framework only.
// TODO(schwerin): Have logger write to a different log from the global log, so that tests can
// redirect their global log output for examination.
template <typename Formatter>
class LogTest : public unittest::Test {
private:
    class LogTestBackend
        : public boost::log::sinks::
              basic_formatted_sink_backend<char, boost::log::sinks::synchronized_feeding> {
    public:
        LogTestBackend(LogTest* ltest) : _ltest(ltest) {}

        void consume(boost::log::record_view const& rec, string_type const& formatted_string) {
            _ltest->_logLines.push_back(formatted_string);
        }

    private:
        LogTest* _ltest;
    };

public:
    LogTest() : _severityOld(globalLogManager()->settings()->getMinimumLogSeverity()) {
        _sink = boost::make_shared<boost::log::sinks::synchronous_sink<LogTestBackend>>(
            boost::make_shared<LogTestBackend>(this));
        _sink->set_formatter(Formatter());
        _sink->set_filter(SeverityFilter());
        boost::log::core::get()->add_sink(_sink);
    }

    virtual ~LogTest() {
        boost::log::core::get()->remove_sink(_sink);
        globalLogManager()->settings()->setMinimumLoggedSeverity(_severityOld);
    }

protected:
    boost::shared_ptr<boost::log::sinks::synchronous_sink<LogTestBackend>> _sink;
    std::vector<std::string> _logLines;
    LogSeverity _severityOld;

private:
    // class LogTestAppender : public MessageLogDomain::EventAppender {
    // public:
    //    explicit LogTestAppender(LogTest* ltest) : _ltest(ltest) {}
    //    virtual ~LogTestAppender() {}
    //    virtual Status append(const MessageLogDomain::Event& event) {
    //        std::ostringstream _os;
    //        if (!_encoder.encode(event, _os))
    //            return Status(ErrorCodes::LogWriteFailed, "Failed to append to LogTestAppender.");
    //        _ltest->_logLines.push_back(_os.str());
    //        return Status::OK();
    //    }

    // private:
    //    LogTest* _ltest;
    //    MessageEventEncoder _encoder;
    //};


    // MessageLogDomain::AppenderHandle _appenderHandle;
};

}  // namespace logger
}  // namespace mongo
