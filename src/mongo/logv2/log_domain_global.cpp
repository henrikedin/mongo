/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include "log_domain_global.h"

#include "mongo/logv2/component_settings_filter.h"
#include "mongo/logv2/console.h"
#include "mongo/logv2/json_formatter.h"
#include "mongo/logv2/log_source.h"
#include "mongo/logv2/ramlog_sink.h"
#include "mongo/logv2/tagged_severity_filter.h"
#include "mongo/logv2/text_formatter.h"

#include <boost/core/null_deleter.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/log/core.hpp>
#include <boost/log/sinks.hpp>

namespace mongo {
namespace logv2 {
namespace {

class RotateCollector : public boost::log::sinks::file::collector {
public:
    explicit RotateCollector(LogDomainGlobal::ConfigurationOptions const& options)
        : _mode{options._fileRotationMode} {}

    void store_file(boost::filesystem::path const& file) override {
        if (_mode == LogDomainGlobal::ConfigurationOptions::RotationMode::kRename) {
            auto renameTarget = file.string() + "." + terseCurrentTime(false);
            boost::system::error_code ec;
            boost::filesystem::rename(file, renameTarget, ec);
            if (ec) {
                // throw here or propagate this error in another way?
            }
        }
    }

    uintmax_t scan_for_files(boost::log::sinks::file::scan_method,
                             boost::filesystem::path const&,
                             unsigned int*) override {
        return 0;
    }

private:
    LogDomainGlobal::ConfigurationOptions::RotationMode _mode;
};

}  // namespace

void LogDomainGlobal::ConfigurationOptions::makeDisabled() {
    _consoleEnabled = false;
}

struct LogDomainGlobal::Impl {
    typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_ostream_backend>
        ConsoleBackend;
    typedef boost::log::sinks::unlocked_sink<RamLogSink> RamLogBackend;
#ifndef _WIN32
    typedef boost::log::sinks::synchronous_sink<boost::log::sinks::syslog_backend> SyslogBackend;
#endif
    typedef boost::log::sinks::synchronous_sink<boost::log::sinks::text_file_backend>
        RotatableFileBackend;

    Impl();
    Status configure(LogDomainGlobal::ConfigurationOptions const& options);
    Status rotate();

    LogComponentSettings _settings;
    boost::shared_ptr<ConsoleBackend> _consoleBackend;
    boost::shared_ptr<RotatableFileBackend> _rotatableFileBackend;
    boost::shared_ptr<RamLogBackend> _globalLogCacheBackend;
    boost::shared_ptr<RamLogBackend> _startupWarningsBackend;
#ifndef _WIN32
    boost::shared_ptr<SyslogBackend> _syslogBackend;
#endif
};

LogDomainGlobal::Impl::Impl() {
    _consoleBackend = boost::make_shared<ConsoleBackend>();
    _consoleBackend->set_filter(ComponentSettingsFilter(_settings));


    _consoleBackend->locked_backend()->add_stream(
        boost::shared_ptr<std::ostream>(&Console::out(), boost::null_deleter()));

    _consoleBackend->locked_backend()->auto_flush();

    _globalLogCacheBackend = RamLogSink::create(RamLog::get("global"));
    _globalLogCacheBackend->set_filter(ComponentSettingsFilter(_settings));

    _startupWarningsBackend = RamLogSink::create(RamLog::get("startupWarnings"));
    _startupWarningsBackend->set_filter(
        TaggedSeverityFilter({LogTag::kStartupWarnings}, LogSeverity::Warning()));

    // Set default configuration
    invariant(configure({}).isOK());

    boost::log::core::get()->add_sink(_globalLogCacheBackend);
    boost::log::core::get()->add_sink(_startupWarningsBackend);
}

Status LogDomainGlobal::Impl::configure(LogDomainGlobal::ConfigurationOptions const& options) {
#ifndef _WIN32
    if (options._syslogEnabled) {
        // Create a backend
        auto backend = boost::make_shared<boost::log::sinks::syslog_backend>(
            boost::log::keywords::facility =
                boost::log::sinks::syslog::make_facility(options._syslogFacility),
            boost::log::keywords::use_impl = boost::log::sinks::syslog::native);

        // // Set the straightforward level translator for the "Severity" attribute of type int
        // backend->set_severity_mapper(
        //     boost::log::sinks::syslog::direct_severity_mapping<int>("Severity"));
        _syslogBackend = boost::make_shared<SyslogBackend>(std::move(backend));
        _syslogBackend->set_filter(ComponentSettingsFilter(_settings));
        _syslogBackend->set_formatter(TextFormatter());
    }
#endif

    if (options._consoleEnabled && _consoleBackend.use_count() == 1) {
        boost::log::core::get()->add_sink(_consoleBackend);
    }

    if (!options._consoleEnabled && _consoleBackend.use_count() > 1) {
        boost::log::core::get()->remove_sink(_consoleBackend);
    }

    if (options._fileEnabled) {
        auto backend = boost::make_shared<boost::log::sinks::text_file_backend>(
            boost::log::keywords::file_name = options._filePath);
        backend->auto_flush(true);

        backend->set_file_collector(boost::make_shared<RotateCollector>(options));

        _rotatableFileBackend = boost::make_shared<RotatableFileBackend>(backend);
        _rotatableFileBackend->set_filter(ComponentSettingsFilter(_settings));
        _rotatableFileBackend->set_formatter(TextFormatter());

        boost::log::core::get()->add_sink(_rotatableFileBackend);
    } else if (_rotatableFileBackend) {
        boost::log::core::get()->remove_sink(_rotatableFileBackend);
        _rotatableFileBackend.reset();
    }

    switch (options._format) {
        case LogFormat::kDefault:
            _consoleBackend->set_formatter(TextFormatter());
            _globalLogCacheBackend->set_formatter(TextFormatter());
            _startupWarningsBackend->set_formatter(TextFormatter());
            if (_rotatableFileBackend)
                _rotatableFileBackend->set_formatter(TextFormatter());
#ifndef _WIN32
            if (_syslogBackend)
                _syslogBackend->set_formatter(TextFormatter());
#endif
            break;

        case LogFormat::kText:
            _consoleBackend->set_formatter(TextFormatter());
            _globalLogCacheBackend->set_formatter(TextFormatter());
            _startupWarningsBackend->set_formatter(TextFormatter());
            if (_rotatableFileBackend)
                _rotatableFileBackend->set_formatter(TextFormatter());
#ifndef _WIN32
            if (_syslogBackend)
                _syslogBackend->set_formatter(TextFormatter());
#endif
            break;

        case LogFormat::kJson:
            _consoleBackend->set_formatter(JsonFormatter());
            _globalLogCacheBackend->set_formatter(JsonFormatter());
            _startupWarningsBackend->set_formatter(JsonFormatter());
            if (_rotatableFileBackend)
                _rotatableFileBackend->set_formatter(JsonFormatter());
#ifndef _WIN32
            if (_syslogBackend)
                _syslogBackend->set_formatter(JsonFormatter());
#endif
            break;
    }

    return Status::OK();
}

Status LogDomainGlobal::Impl::rotate() {
    if (_rotatableFileBackend) {
        auto backend = _rotatableFileBackend->locked_backend();
        backend->rotate_file();
    }
    return Status::OK();
}

LogDomainGlobal::LogDomainGlobal() {
    _impl = std::make_unique<Impl>();
}

LogDomainGlobal::~LogDomainGlobal() {}

LogSource& LogDomainGlobal::source() {
    // Use a thread_local logger so we don't need to have locking
    thread_local LogSource lg;
    return lg;
}

boost::shared_ptr<boost::log::core> LogDomainGlobal::core() {
    return boost::log::core::get();
}

Status LogDomainGlobal::configure(LogDomainGlobal::ConfigurationOptions const& options) {
    return _impl->configure(options);
}

Status LogDomainGlobal::rotate() {
    return _impl->rotate();
}

LogComponentSettings& LogDomainGlobal::settings() {
    return _impl->_settings;
}

}  // namespace logv2
}  // namespace mongo
