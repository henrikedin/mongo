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

// #pragma once is not used in this header.
// This header attempts to enforce the rule that no logging should be done in
// an inline function defined in a header.
// To enforce this "no logging in header" rule, we use #include guards with a validating #else
// clause.
// Also, this header relies on a preprocessor macro to determine the default component for the
// unconditional logging functions severe(), error(), warning() and log(). Disallowing multiple
// inclusion of log.h will ensure that the default component will be set correctly.

#if defined(MONGO_UTIL_LOG_H_)
#error \
    "mongo/util/log.h cannot be included multiple times. " \
       "This may occur when log.h is included in a header. " \
       "Please check your #include's."
#else  // MONGO_UTIL_LOG_H_
#define MONGO_UTIL_LOG_H_

#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/logger/attribute_payload.h"
#include "mongo/logger/log_component.h"
#include "mongo/logger/log_severity_limiter.h"
#include "mongo/logger/log_source.h"
#include "mongo/logger/logger.h"
#include "mongo/logger/logstream_builder.h"
#include "mongo/logger/redaction.h"
#include "mongo/logger/tee.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/errno_util.h"
#include <boost/log/utility/manipulators/add_value.hpp>
#include <type_traits>

// Provide log component in global scope so that MONGO_LOG will always have a valid component.
// Global log component will be kDefault unless overridden by MONGO_LOG_DEFAULT_COMPONENT.
#if defined(MONGO_LOG_DEFAULT_COMPONENT)
const ::mongo::logger::LogComponent MongoLogDefaultComponent_component =
    MONGO_LOG_DEFAULT_COMPONENT;
#else
#error \
    "mongo/util/log.h requires MONGO_LOG_DEFAULT_COMPONENT to be defined. " \
       "Please see http://www.mongodb.org/about/contributors/reference/server-logging-rules/ "
#endif  // MONGO_LOG_DEFAULT_COMPONENT

namespace mongo {

namespace logger {
typedef void (*ExtraLogContextFn)(BufBuilder& builder);
Status registerExtraLogContextFn(ExtraLogContextFn contextFn);

}  // namespace logger

namespace {

using logger::LogstreamBuilder;
using logger::Tee;

/**
 * Returns a LogstreamBuilder for logging a message with LogSeverity::Severe().
 */
inline LogstreamBuilder severe(logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(/*logger::globalLogDomain(),*/
                            /*getThreadName(),*/
                            domain,
                            logger::LogSeverity::Severe(),
                            ::MongoLogDefaultComponent_component);
}

inline LogstreamBuilder severe(logger::LogComponent component,
                               logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(
        domain,
        /*logger::globalLogDomain(),*/ /*getThreadName(),*/ logger::LogSeverity::Severe(),
        component);
}

/**
 * Returns a LogstreamBuilder for logging a message with LogSeverity::Error().
 */
inline LogstreamBuilder error(logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(domain, /*logger::globalLogDomain(),*/
                            /*getThreadName(),*/
                            logger::LogSeverity::Error(),
                            ::MongoLogDefaultComponent_component);
}

inline LogstreamBuilder error(logger::LogComponent component,
                              logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(
        domain,
        /*logger::globalLogDomain(),*/ /*getThreadName(),*/ logger::LogSeverity::Error(),
        component);
}

/**
 * Returns a LogstreamBuilder for logging a message with LogSeverity::Warning().
 */
inline LogstreamBuilder warning(logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(domain, /*logger::globalLogDomain(),*/
                            /*getThreadName(),*/
                            logger::LogSeverity::Warning(),
                            ::MongoLogDefaultComponent_component);
}

inline LogstreamBuilder warning(logger::LogComponent component,
                                logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(
        domain,
        /*logger::globalLogDomain(),*/ /*getThreadName(),*/ logger::LogSeverity::Warning(),
        component);
}

/**
 * Returns a LogstreamBuilder for logging a message with LogSeverity::Log().
 */
inline LogstreamBuilder log(logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(domain, /*logger::globalLogDomain(),*/
                            /*getThreadName(),*/
                            logger::LogSeverity::Log(),
                            ::MongoLogDefaultComponent_component);
}

/**
 * Returns a LogstreamBuilder that does not cache its ostream in a threadlocal cache.
 * Use this variant when logging from places that may not be able to access threadlocals,
 * such as from within other threadlocal-managed objects, or thread_specific_ptr-managed
 * objects.
 *
 * Once SERVER-29377 is completed, this overload can be removed.
 */
inline LogstreamBuilder logNoCache(logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(domain, /*logger::globalLogDomain(),*/
                            /*getThreadName(),*/
                            logger::LogSeverity::Log(),
                            ::MongoLogDefaultComponent_component,
                            false);
}

inline LogstreamBuilder log(logger::LogComponent component,
                            logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(
        domain,
        /*logger::globalLogDomain(),*/ /*getThreadName(),*/ logger::LogSeverity::Log(),
        component);
}

inline LogstreamBuilder log(logger::LogComponent::Value componentValue,
                            logger::LogDomain domain = logger::kDefault) {
    return LogstreamBuilder(
        domain,
        /*logger::globalLogDomain(),*/ /*getThreadName(),*/ logger::LogSeverity::Log(),
        componentValue);
}

/**
 * Runs the same logic as log()/warning()/error(), without actually outputting a stream.
 */
inline bool shouldLog(logger::LogComponent logComponent, logger::LogSeverity severity) {
    return logger::globalLogManager()->settings()->shouldLog(logComponent, severity);
}

inline bool shouldLog(logger::LogSeverity severity) {
    return shouldLog(::MongoLogDefaultComponent_component, severity);
}

}  // namespace

// MONGO_LOG uses log component from MongoLogDefaultComponent from current or global namespace.
#define MONGO_LOG(DLEVEL)                                                                          \
    if (!(::mongo::logger::globalLogManager()->settings())                                         \
             ->shouldLog(MongoLogDefaultComponent_component,                                       \
                         ::mongo::LogstreamBuilder::severityCast(DLEVEL))) {                       \
    } else                                                                                         \
        ::mongo::logger::LogstreamBuilder(::mongo::logger::kDefault, /*::mongo::getThreadName(),*/ \
                                          ::mongo::LogstreamBuilder::severityCast(DLEVEL),         \
                                          MongoLogDefaultComponent_component)

#define LOG MONGO_LOG

#define MONGO_LOG_COMPONENT(DLEVEL, COMPONENT1)                                                    \
    if (!(::mongo::logger::globalLogManager()->settings())                                         \
             ->shouldLog((COMPONENT1), ::mongo::LogstreamBuilder::severityCast(DLEVEL))) {         \
    } else                                                                                         \
        ::mongo::logger::LogstreamBuilder(::mongo::logger::kDefault, /*::mongo::getThreadName(),*/ \
                                          ::mongo::LogstreamBuilder::severityCast(DLEVEL),         \
                                          (COMPONENT1))

/**
 * Rotates the log files.  Returns true if all logs rotate successfully.
 *
 * renameFiles - true means we rename files, false means we expect the file to be renamed
 *               externally
 *
 * logrotate on *nix systems expects us not to rename the file, it is expected that the program
 * simply open the file again with the same name.
 * We expect logrotate to rename the existing file before we rotate, and so the next open
 * we do should result in a file create.
 */
bool rotateLogs(bool renameFiles);

// extern Tee* const warnings;            // Things put here go in serverStatus
// extern Tee* const startupWarningsLog;  // Things put here get reported in MMS

/**
 * Write the current context (backtrace), along with the optional "msg".
 */
void logContext(const char* msg = NULL);

/**
 * Turns the global log manager into a plain console logger (no adornments).
 */
void setPlainConsoleLogger();


struct LogPrototype1BuilderStage2 {
    LogPrototype1BuilderStage2(const char* c, std::size_t len) : str(c), length(len) {}

    const char* str;
    std::size_t length;
    boost::log::attribute_value_set attrs;
};


struct LogPrototype1BuilderStage1 {
    LogPrototype1BuilderStage1(const char* c, std::size_t len) : str(c), length(len) {}

    template <typename... Args>
    LogPrototype1BuilderStage2 operator()(Args... arg) {
        LogPrototype1BuilderStage2 stage2(str, length);
        // add_attribute(stage2.attrs, std::move(arg));
        (add_attribute(stage2.attrs, arg), ...);
        //(stage2.attrs[arg.get_name()] = arg.get_value(), ...);
        return stage2;
    }

    template <typename RefT>
    void add_attribute(boost::log::attribute_value_set& attrs,
                       boost::log::add_value_manip<RefT> arg) {
        typedef typename boost::log::aux::make_embedded_string_type<
            typename boost::log::add_value_manip<RefT>::value_type>::type value_type;
        boost::log::attribute_value value(
            new boost::log::attributes::attribute_value_impl<value_type>(arg.get_value()));
        attrs.insert(arg.get_name(), value);
    }

    const char* str;
    std::size_t length;
};

inline LogPrototype1BuilderStage1 operator"" _log1(const char* c, std::size_t len) {
    return LogPrototype1BuilderStage1(c, len);
}

void log_prototype1(logger::LogComponent component,
                    logger::LogSeverity severity,
                    LogPrototype1BuilderStage2&& stage2);

template <typename RefT>
struct NamedAttribute {

    typedef RefT reference_type;
    typedef std::remove_cv_t<std::remove_reference_t<reference_type>> value_type;
    typedef std::conditional_t<std::is_scalar_v<value_type>, value_type, reference_type>
        storage_type;

    NamedAttribute(const char* n, reference_type v) : name(n), value(v) {}

    const char* name;
    storage_type value;
};

struct UdlAttribute {
    UdlAttribute(const char* s) : name(s) {}

    template <typename T>
    NamedAttribute<T&&> operator=(T&& value) const {
        return {name, std::forward<T>(value)};
    }

    const char* name;
};

struct extended_udl_arg : public fmt::internal::udl_arg<char> {
    using fmt::internal::udl_arg<char>::operator=;

    fmt::internal::named_arg<fmt::string_view, char> operator=(::mongo::StringData sd) const {
        return {str, fmt::string_view(sd.rawData(), sd.size())};
    }

};

inline extended_udl_arg operator""_attr(const char* c, std::size_t len) {
    return {c};
}

// template <typename RefT>
// void log_prototype2_add_attribute(boost::log::attribute_value_set& attrs,
//                                  NamedAttribute<RefT>&& arg) {
//    typedef typename boost::log::aux::make_embedded_string_type<
//        typename NamedAttribute<RefT>::value_type>::type value_type;
//    boost::log::attribute_value value(
//        new boost::log::attributes::attribute_value_impl<value_type>(arg.value));
//    attrs.insert(arg.name, value);
//}
//
// template <typename... Args>
// void log_prototype2(logger::LogComponent component,
//                    logger::LogSeverity severity,
//                    const char* message,
//                    Args... args) {
//    LogstreamBuilder logstrm(::mongo::logger::kDefault, severity, component);
//    if (logstrm._rec) {
//        (log_prototype2_add_attribute(logstrm._rec.attribute_values(), std::forward<Args>(args)),
//         ...);
//        logstrm._rec.attribute_values().insert(
//            "message",
//            boost::log::attribute_value(
//                new boost::log::attributes::attribute_value_impl<const char*>(message)));
//    }
//}

// template <typename T>
// void log_prototype3_add_attribute(boost::log::attribute_value_set& attrs,
//                                  fmt::internal::named_arg<T, char>&& arg) {
//    typedef typename boost::log::aux::make_embedded_string_type<
//        std::remove_cv_t<std::remove_reference_t<T>>>::type value_type;
//    boost::log::attribute_value value(
//        new boost::log::attributes::attribute_value_impl<value_type>(arg.value));
//    attrs.insert(std::string(arg.name.data(), arg.name.size()), value);
//}
//
// template <typename... Args>
// constexpr void log_prototype3(logger::LogComponent component,
//                              logger::LogSeverity severity,
//                              const char* message,
//                              Args... args) {
//    auto as = fmt::internal::make_args_checked(message, args...);
//    auto as2 = fmt::basic_format_args<typename fmt::buffer_context<char>::type>{as};
//
//    {
//        LogstreamBuilder logstrm(::mongo::logger::kDefault, severity, component);
//        if (logstrm._rec) {
//            (log_prototype3_add_attribute(logstrm._rec.attribute_values(),
//                                          std::forward<Args>(args)),
//             ...);
//            logstrm._rec.attribute_values().insert(
//                attributes::message(),
//                boost::log::attribute_value(
//                    new boost::log::attributes::attribute_value_impl<std::string>(message)));
//
//            logstrm._rec.attribute_values().insert(
//                "formatting",
//                boost::log::attribute_value(
//                    new boost::log::attributes::attribute_value_impl<decltype(as2)>(as2)));
//        }
//    }
//}

void log_prototype4_impl(logger::LogComponent component,
                         logger::LogSeverity severity,
                         std::string message,
                         const ::mongo::logger::AttributePayload& payload);

void log_prototype4_helper_impl(boost::log::record record,
                                std::string message,
                                const ::mongo::logger::AttributePayload& payload);

template <typename... Args>
void log_prototype4_helper(boost::log::record record, const char* message, Args&&... args) {
    auto args_checked = fmt::internal::make_args_checked(message, std::forward<Args>(args)...);
    ::mongo::logger::AttributePayload payload;
    (payload.names.emplace_back(args.name.data(), args.name.size()), ...);
    payload.values = fmt::basic_format_args<typename fmt::buffer_context<char>::type>{args_checked};
    log_prototype4_helper_impl(std::move(record), message, payload);
}

template <typename S, typename... Args>
constexpr void log_prototype4(logger::LogComponent component,
                              logger::LogSeverity severity,
                              const S& message,
                              Args&&... args) {
    auto args_checked = fmt::internal::make_args_checked(message, std::forward<Args>(args)...);
    ::mongo::logger::AttributePayload payload;
    (payload.names.emplace_back(args.name.data(), args.name.size()), ...);
    payload.values = fmt::basic_format_args<typename fmt::buffer_context<char>::type>{args_checked};
    log_prototype4_impl(component, severity, message, payload);
}

template <typename S, typename... Args>
constexpr void log_prototype4(logger::LogSeverity severity, const S& message, Args&&... args) {
    log_prototype4(
        ::MongoLogDefaultComponent_component, severity, message, std::forward<Args>(args)...);
}

template <typename... Args>
constexpr void log_prototype4(const char* message, Args&&... args) {
    log_prototype4(::MongoLogDefaultComponent_component,
                   logger::LogSeverity::Log(),
                   std::string(message),
                   std::forward<Args>(args)...);
}

template <typename... Args>
constexpr void log_prototype4(std::string message, Args&&... args) {
    log_prototype4(::MongoLogDefaultComponent_component,
                   logger::LogSeverity::Log(),
                   std::move(message),
                   std::forward<Args>(args)...);
}

template <typename... Args>
constexpr void log_prototype4(::mongo::StringData sd, Args&&... args) {
    log_prototype4(::MongoLogDefaultComponent_component,
                   logger::LogSeverity::Log(),
                   sd.toString(),
                   std::forward<Args>(args)...);
}

::mongo::logger::log_source& threadLogSource();

#define MONGO_DEBUG_LOG(DLEVEL, ...)                                                       \
    if (auto record =                                                                      \
            threadLogSource().open_record(::mongo::logger::kDefault,                       \
                                          ::mongo::LogstreamBuilder::severityCast(DLEVEL), \
                                          MongoLogDefaultComponent_component))             \
    log_prototype4_helper(std::move(record), __VA_ARGS__)

}  // namespace mongo

#endif  // MONGO_UTIL_LOG_H_
