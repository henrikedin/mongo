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

#include "mongo/logger/logstream_builder.h"

#include <memory>

#include "mongo/base/init.h"
#include "mongo/base/status.h"
#include "mongo/logger/message_event_utf8_encoder.h"
#include "mongo/logger/tee.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"  // TODO: remove apple dep for this in threadlocal.h
#include "mongo/util/time_support.h"

//#include <boost/log/trivial.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/expressions.hpp>
#include <boost/make_shared.hpp>
#include <boost/core/null_deleter.hpp>
#include <boost/log/utility/setup/formatter_parser.hpp>
#include <boost/log/attributes/function.hpp>
#include "mongo/util/concurrency/thread_name.h"
#include <boost/phoenix/bind.hpp>

#include <fstream>

namespace mongo {

namespace {

/// This flag indicates whether the system providing a per-thread cache of ostringstreams
/// for use by LogstreamBuilder instances is initialized and ready for use.  Until this
/// flag is true, LogstreamBuilder instances must not use the cache.
bool isThreadOstreamCacheInitialized = false;

MONGO_INITIALIZER(LogstreamBuilder)(InitializerContext*) {
    isThreadOstreamCacheInitialized = true;
    return Status::OK();
}

thread_local std::unique_ptr<std::ostringstream> threadOstreamCache;

// During unittests, where we don't use quickExit(), static finalization may destroy the
// cache before its last use, so mark it as not initialized in that case.
// This must be after the definition of threadOstreamCache so that it is destroyed first.
struct ThreadOstreamCacheFinalizer {
    ~ThreadOstreamCacheFinalizer() {
        isThreadOstreamCacheInitialized = false;
    }
} threadOstreamCacheFinalizer;

}  // namespace

namespace logger {

LogstreamBuilder::LogstreamBuilder(MessageLogDomain* domain,
                                   StringData contextName,
                                   LogSeverity severity)
    : LogstreamBuilder(domain, contextName, std::move(severity), LogComponent::kDefault) {}

LogstreamBuilder::LogstreamBuilder(MessageLogDomain* domain,
                                   StringData contextName,
                                   LogSeverity severity,
                                   LogComponent component,
                                   bool shouldCache)
    : _domain(domain),
      _contextName(contextName.toString()),
      _severity(std::move(severity)),
      _component(std::move(component)),
      _tee(nullptr),
      _shouldCache(shouldCache) {}

LogstreamBuilder::LogstreamBuilder(logger::MessageLogDomain* domain,
                                   StringData contextName,
                                   LabeledLevel labeledLevel)
    : LogstreamBuilder(domain, contextName, static_cast<LogSeverity>(labeledLevel)) {
    setBaseMessage(labeledLevel.getLabel());
}

LogstreamBuilder::~LogstreamBuilder() {
    //if (_os) {
    //    if (!_baseMessage.empty())
    //        _baseMessage.push_back(' ');
    //    _baseMessage += _os->str();
    //    MessageEventEphemeral message(
    //        Date_t::now(), _severity, _component, _contextName, _baseMessage);
    //    message.setIsTruncatable(_isTruncatable);
    //    _domain->append(message).transitional_ignore();
    //    if (_tee) {
    //        _os->str("");
    //        logger::MessageEventDetailsEncoder teeEncoder;
    //        teeEncoder.encode(message, *_os);
    //        _tee->write(_os->str());
    //    }
    //    _os->str("");
    //    if (_shouldCache && isThreadOstreamCacheInitialized && !threadOstreamCache) {
    //        threadOstreamCache = std::move(_os);
    //    }
    //}
}

void LogstreamBuilder::operator<<(Tee* tee) {
    makeStream();  // Adding a Tee counts for purposes of deciding to make a log message.
    // TODO: dassert(!_tee);
    _tee = tee;
}

void LogstreamBuilder::makeStream() {
    if (!_os) {
        if (_shouldCache && isThreadOstreamCacheInitialized && threadOstreamCache) {
            _os = std::move(threadOstreamCache);
        } else {
            _os = stdx::make_unique<std::ostringstream>();
        }
    }
}

}  // namespace logger
}  // namespace mongo

/*
BOOST_LOG_ATTRIBUTE_KEYWORD(a_severity, "Severity", mongo::logger::LogSeverity)
typedef boost::log::sources::severity_channel_logger_mt<mongo::logger::LogSeverity, mongo::logger::LogComponent> logger_type;

BOOST_LOG_INLINE_GLOBAL_LOGGER_INIT(my_logger, logger_type)
{
// Do something that needs to be done on logger initialization,
// e.g. add a stop watch attribute.
	boost::log::sources::severity_channel_logger_mt<mongo::logger::LogSeverity, mongo::logger::LogComponent> lg;
//lg.add_attribute("StopWatch", boost::make_shared< attrs::timer >());
// The initializing routine must return the logger instance

	lg.add_attribute("TimeStamp", boost::log::attributes::make_function([]() {
		return mongo::Date_t::now();
	}));

	lg.add_attribute("ThreadName", boost::log::attributes::make_function([]() {
		return mongo::getThreadName();
	}));
	

	return lg;
}

char severity_format(boost::log::value_ref< mongo::logger::LogSeverity > const& severity)
{
	// Check to see if the attribute value has been found
	//if (severity)
	//return boost::filesystem::path(filename.get()).filename().string();
	//else
	//return std::string();
	return severity.get().toChar();
};

void severity_format2(boost::log::record_view const& record, boost::log::formatting_ostream& strm)
{
	// Check to see if the attribute value has been found
	boost::log::value_ref< mongo::logger::LogSeverity, tag::a_severity > severity = record[a_severity];
	if (severity)
		strm << severity.get().toChar();
};


boost::log::sources::severity_channel_logger_mt<mongo::logger::LogSeverity, mongo::logger::LogComponent>& get_logger()
{
	//static boost::log::sources::severity_logger_mt<> logger;
	
	//return logger;
	static bool first = true;
	if (first)
	{
		//boost::log::register_simple_formatter_factory< severity_level, char >("Severity");
		//boost::log::register_formatter_factory< mongo::logger::LogSeverity, char >("Severity");
		//boost::log::core::get()->add_global_attribute("TimeStamp", boost::log::attributes::make_function([]() {
		//	return mongo::Date_t::now();
		//}));

		typedef boost::log::sinks::asynchronous_sink< boost::log::sinks::text_ostream_backend > text_sink;
		boost::shared_ptr< text_sink > sink = boost::make_shared< text_sink >();

		//sink->locked_backend()->add_stream(
		//	boost::make_shared< std::ofstream >("sample.log"));

		sink->locked_backend()->add_stream(
			boost::shared_ptr< std::ostream >(&std::cout, boost::null_deleter()));

		sink->set_formatter
		(
			boost::log::expressions::stream
			// line id will be written in hex, 8-digits, zero-filled
			//<< std::hex << std::setw(8) << std::setfill('0') << boost::log::expressions::attr< unsigned int >("LineID")
			//<< ": <" << logging::trivial::severity
			
			<< boost::log::expressions::attr< mongo::Date_t >("TimeStamp") << " "

			//<< boost::log::expressions::attr< mongo::logger::LogSeverity >("Severity") << " "
			//<< boost::phoenix::bind(&severity_format, boost::log::expressions::attr< mongo::logger::LogSeverity >("Severity")) << " "
			<< boost::log::expressions::wrap_formatter(&severity_format2) << " "

			<< boost::log::expressions::attr< mongo::logger::LogComponent >("Channel") << " ["
			<< boost::log::expressions::attr< mongo::StringData >("ThreadName") << "] "
			<< boost::log::expressions::smessage

		);

		boost::log::core::get()->add_sink(sink);

		first = false;
	}
	return my_logger::get();
	
	// TODO: insert return statement here
}
*/
