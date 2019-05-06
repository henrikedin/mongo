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

#include <boost/optional.hpp>
#include <memory>
#include <sstream>
#include <string>

#include "mongo/base/error_codes.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/logger/log_component.h"
#include "mongo/logger/log_severity.h"
#include "mongo/logger/message_log_domain.h"
#include "mongo/stdx/chrono.h"
#include "mongo/util/exit_code.h"

#include <boost/log/core/record.hpp>
#include <boost/log/sources/record_ostream.hpp>

namespace mongo {
namespace logger {

class Tee;

/**
 * Stream-ish object used to build and append log messages.
 */
class LogstreamBuilder {
public:
    static LogSeverity severityCast(int ll) {
        return LogSeverity::cast(ll);
    }
    static LogSeverity severityCast(LogSeverity ls) {
        return ls;
    }

    /**
     * Construct a LogstreamBuilder that writes to "domain" on destruction.
     *
     * "contextName" is a short name of the thread or other context.
     * "severity" is the logging severity of the message.
     */
    LogstreamBuilder(/*MessageLogDomain* domain,*/ /*StringData contextName, */LogSeverity severity);

    /**
     * Construct a LogstreamBuilder that writes to "domain" on destruction.
     *
     * "contextName" is a short name of the thread or other context.
     * "severity" is the logging severity of the message.
     * "component" is the primary log component of the message.
     *
     * By default, this class will create one ostream per thread, and it
     * will cache that object in a threadlocal and reuse it for subsequent
     * logs messages. Set "shouldCache" to false to create a new ostream
     * for each instance of this class rather than cacheing.
     */
    LogstreamBuilder(/*MessageLogDomain* domain,*/
                     /*StringData contextName,*/
                     LogSeverity severity,
                     LogComponent component,
                     bool shouldCache = true);

    LogstreamBuilder(LogstreamBuilder&& other) = default;
    LogstreamBuilder& operator=(LogstreamBuilder&& other) = default;

    /**
     * Destroys a LogstreamBuilder().  If anything was written to it via stream() or operator<<,
     * constructs a MessageLogDomain::Event and appends it to the associated domain.
     */
    ~LogstreamBuilder();


    /**
     * Sets an optional prefix for the message.
     */
    LogstreamBuilder& setBaseMessage(const std::string& baseMessage) {
        //_baseMessage = baseMessage;
        return *this;
    }

    LogstreamBuilder& setIsTruncatable(bool isTruncatable) {
        //_isTruncatable = isTruncatable;
        return *this;
    }

    boost::log::record_ostream& stream() {
        /*if (!_os)
            makeStream();
        return *_os;*/
        return *_recStream;
    }

    LogstreamBuilder& operator<<(const char* x) {
		if (_rec)
		{
            (*_recStream) << x;
		}
        return *this;
    }
    LogstreamBuilder& operator<<(const std::string& x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(StringData x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(char* x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(char x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(int x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(ExitCode x) {
        if (_rec)
            (*_recStream) << x;
        return *this;
    }
    LogstreamBuilder& operator<<(long x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(unsigned long x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(unsigned x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(unsigned short x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(double x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(void* x) {
        if (_rec)
            (*_recStream) << x;
        return *this;
    }
    LogstreamBuilder& operator<<(const void* x) {
        if (_rec)
            (*_recStream) << x;
        return *this;
    }
    LogstreamBuilder& operator<<(long long x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(unsigned long long x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }
    LogstreamBuilder& operator<<(bool x) {
        if (_rec) {
            (*_recStream) << x;
        }
        return *this;
    }

    template <typename Period>
    LogstreamBuilder& operator<<(const Duration<Period>& d) {
        if (_rec)
			(*_recStream) << d;

        return *this;
    }

    LogstreamBuilder& operator<<(BSONType t) {
        if (_rec)
            (*_recStream) << typeName(t);
        return *this;
    }

    LogstreamBuilder& operator<<(ErrorCodes::Error ec) {
        if (_rec)
            (*_recStream) << ErrorCodes::errorString(ec);
        return *this;
    }

    template <typename T>
    LogstreamBuilder& operator<<(const T& x) {
        if (_rec)
            (*_recStream) << x.toString();
        return *this;
    }

    /*LogstreamBuilder& operator<<(std::ostream& (*manip)(std::ostream&)) {
        stream() << manip;
        return *this;
    }*/
    LogstreamBuilder& operator<<(std::ios_base& (*manip)(std::ios_base&)) {
        if (_rec)
            (*_recStream) << manip;
        return *this;
    }

    template <typename OptionalType>
    LogstreamBuilder& operator<<(const boost::optional<OptionalType>& optional) {
        if (optional) {
            (*this << *optional);
        } else {
            (*this << "(nothing)");
        }
        return *this;
    }

    /**
     * In addition to appending the message to _domain, write it to the given tee.  May only
     * be called once per instance of LogstreamBuilder.
     */
    void operator<<(Tee* tee);

private:
    void makeStream();

    boost::log::record _rec;
    std::unique_ptr<boost::log::record_ostream> _recStream;

   /* MessageLogDomain* _domain;
    std::string _contextName;
    LogSeverity _severity;
    LogComponent _component;
    std::string _baseMessage;
    std::unique_ptr<std::ostringstream> _os;
    Tee* _tee;
    bool _isTruncatable = true;
    bool _shouldCache;*/
};


}  // namespace logger
}  // namespace mongo
