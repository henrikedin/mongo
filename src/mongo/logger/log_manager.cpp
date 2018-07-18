/*    Copyright 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/logger/log_manager.h"

#include "mongo/logger/console.h"
#include "mongo/logger/console_appender.h"
#include "mongo/logger/message_event_utf8_encoder.h"

#include <boost/log/attributes/function.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/utility/setup/formatter_parser.hpp>

#include <boost/log/sinks/event_log_backend.hpp>

//#include <boost/core/null_deleter.hpp>

namespace mongo {
namespace logger {

	namespace {
		BOOST_LOG_ATTRIBUTE_KEYWORD(a_severity, "Severity", LogSeverity)

		typedef boost::log::sinks::asynchronous_sink< boost::log::sinks::text_ostream_backend > async_text_sink;
		boost::shared_ptr< async_text_sink > sink;

		typedef boost::log::sinks::synchronous_sink< boost::log::sinks::simple_event_log_backend > win_sink_t;

		typedef boost::log::sinks::synchronous_sink< boost::log::sinks::debug_output_backend > debug_sink_t;


		void severity_formatter(boost::log::record_view const& record, boost::log::formatting_ostream& strm)
		{
			// Check to see if the attribute value has been found
			boost::log::value_ref< mongo::logger::LogSeverity, tag::a_severity > severity = record[a_severity];
			strm << severity.get().toChar();
		};
	}

LogManager::LogManager() {
	_DefaultLogger.add_attribute("TimeStamp", boost::log::attributes::make_function([]() {
		return mongo::Date_t::now();
	}));

	_DefaultLogger.add_attribute("ThreadName", boost::log::attributes::make_function([]() {
		return mongo::getThreadName();
	}));

	sink = boost::make_shared< async_text_sink >();

	sink->locked_backend()->add_stream(Console::create());

	sink->set_formatter
	(
		boost::log::expressions::stream

		<< boost::log::expressions::attr< mongo::Date_t >("TimeStamp") << " "
		<< boost::log::expressions::wrap_formatter(&severity_formatter) << " "
		<< boost::log::expressions::attr< mongo::logger::LogComponent >("Channel") << " ["
		<< boost::log::expressions::attr< mongo::StringData >("ThreadName") << "] "
		<< boost::log::expressions::smessage
	);

	boost::log::core::get()->add_sink(sink);


	boost::shared_ptr< debug_sink_t > debugsink(new debug_sink_t());

	debugsink->set_filter(boost::log::expressions::is_debugger_present());
	debugsink->set_formatter
	(
		boost::log::expressions::stream

		<< boost::log::expressions::attr< mongo::Date_t >("TimeStamp") << " "
		<< boost::log::expressions::wrap_formatter(&severity_formatter) << " "
		<< boost::log::expressions::attr< mongo::logger::LogComponent >("Channel") << " ["
		<< boost::log::expressions::attr< mongo::StringData >("ThreadName") << "] "
		<< boost::log::expressions::smessage << "\n"
	);

	boost::log::core::get()->add_sink(debugsink);

	boost::shared_ptr< win_sink_t > winsink(new win_sink_t());

	winsink->set_formatter
	(
		boost::log::expressions::format("%1%")
		% boost::log::expressions::smessage
	);

	// We'll have to map our custom levels to the event log event types
	boost::log::sinks::event_log::custom_event_type_mapping< LogSeverity > mapping("Severity");
	mapping[LogSeverity::Info()] = boost::log::sinks::event_log::info;
	mapping[LogSeverity::Warning()] = boost::log::sinks::event_log::warning;
	mapping[LogSeverity::Error()] = boost::log::sinks::event_log::error;

	winsink->locked_backend()->set_event_type_mapper(mapping);

	// Add the sink to the core
	boost::log::core::get()->add_sink(winsink);
}

LogManager::~LogManager() {
    for (DomainsByNameMap::iterator iter = _domains.begin(); iter != _domains.end(); ++iter) {
        delete iter->second;
    }
}

void LogManager::StopAndFlush()
{
	boost::shared_ptr< boost::log::core > core = boost::log::core::get();

	// Remove the sink from the core, so that no records are passed to it
	core->remove_sink(sink);

	// Break the feeding loop
	sink->stop();

	// Flush all log records that may have left buffered
	sink->flush();

	sink.reset();

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
