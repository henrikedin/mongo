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
// unconditional logging functions severe(), error(), warning() and LOG(). Disallowing multiple
// inclusion of log.h will ensure that the default component will be set correctly.

#if defined(MONGO_UTIL_LOG_H_)
#error \
    "mongo/log/log.h cannot be included multiple times. " \
       "This may occur when log.h is included in a header. " \
       "Please check your #include's."
#else  // MONGO_UTIL_LOG_H_
#define MONGO_UTIL_LOG_H_

#include "mongo/base/status.h"
#include "mongo/bson/util/builder.h"
#include "mongo/log/log_component.h"
#include "mongo/log/log_component_settings.h"
#include "mongo/log/log_debug.h"
#include "mongo/log/log_detail.h"
#include "mongo/log/log_domain.h"
#include "mongo/log/log_options.h"
#include "mongo/log/log_severity.h"
#include "mongo/log/redaction.h"
#include "mongo/util/errno_util.h"

namespace {
#if defined(MONGO_LOG_DEFAULT_COMPONENT)
// Provide log component in global scope so that MONGO_LOG will always have a valid component.
// Global log component will be kDefault unless overridden by MONGO_LOG_DEFAULT_COMPONENT.
const mongo::log::LogComponent MongologDefaultComponent_component = MONGO_LOG_DEFAULT_COMPONENT;
#else
#error \
    "mongo/log/log.h requires MONGO_LOG_DEFAULT_COMPONENT to be defined. " \
       "Please see https://github.com/mongodb/mongo/blob/master/src/mongo/log/README.md "
#endif  // MONGO_LOG_DEFAULT_COMPONENT
}  // namespace

// The logging macros below are documented in detail under src/mongo/log/README.md
namespace mongo {
// Internal helper to be able to create LogOptions with two arguments from other macros
#define MAKE_OPTIONS_ARG2(ARG0, ARG1) \
    ::mongo::log::LogOptions {        \
        ARG0, ARG1                    \
    }

// Internal helper to perform the logging where it requires the MESSAGE to be a compile time string.
#define MONGO_LOG_IMPL(ID, SEVERITY, OPTIONS, FMTSTR_MESSAGE, ...) \
    ::mongo::log::detail::doLog(ID, SEVERITY, OPTIONS, FMT_STRING(FMTSTR_MESSAGE), ##__VA_ARGS__)

/**
 * Log with default severity and component.
 *
 * This macro acts like a function with 4 overloads:
 *   LOG(ID, FMTSTR_MESSAGE, ATTRIBUTES...)
 *   LOG(ID, FMTSTR_MESSAGE, DYNAMIC_ATTRIBUTES)
 *   LOG(ID, FMTSTR_MESSAGE, MESSAGE, ATTRIBUTES...)
 *   LOG(ID, FMTSTR_MESSAGE, MESSAGE, DYNAMIC_ATTRIBUTES)
 *
 * ID is a unique signed int32 in the same number space as other error codes.
 * FMTSTR_MESSAGE is a compile time string constant. Regular "string" is preferred.
 *   This string may contain libfmt replacement fields.
 * MESSAGE is an optional compile time string constant of message without libfmt replacement fields
 * ATTRIBUTES zero more more static attributes created with "name"_attr=value expressions
 * DYNAMIC_ATTRIBUTES single argument DynamicAttributes object
 *   no attributes may be passed with "name"_attr=value when this is used
 */
#define MONGO_LOG(ID, FMTSTR_MESSAGE, ...)                                       \
    MONGO_LOG_IMPL(ID,                                                           \
                   ::mongo::log::LogSeverity::Log(),                             \
                   ::mongo::log::LogOptions{MongologDefaultComponent_component}, \
                   FMTSTR_MESSAGE,                                               \
                   ##__VA_ARGS__)

/**
 * Log with default severity and custom options.
 *
 * OPTIONS is an expression that is used to construct a LogOptions.
 * See LogOptions for available parameters when performing custom logging
 *
 * See LOG() for documentation of the other parameters
 */
#define MONGO_LOG_OPTIONS(ID, OPTIONS, FMTSTR_MESSAGE, ...)          \
    MONGO_LOG_IMPL(ID,                                               \
                   ::mongo::log::LogSeverity::Log(),                 \
                   ::mongo::log::LogOptions::ensureValidComponent(   \
                       OPTIONS, MongologDefaultComponent_component), \
                   FMTSTR_MESSAGE,                                   \
                   ##__VA_ARGS__)

/**
 * Log with info severity.
 *
 * See LOG() for documentation of the parameters
 */
#define MONGO_LOG_INFO(ID, FMTSTR_MESSAGE, ...)                                  \
    MONGO_LOG_IMPL(ID,                                                           \
                   ::mongo::log::LogSeverity::Info(),                            \
                   ::mongo::log::LogOptions{MongologDefaultComponent_component}, \
                   FMTSTR_MESSAGE,                                               \
                   ##__VA_ARGS__)

/**
 * Log with info severity and custom options.
 *
 * See LOG_OPTIONS() for documentation of the parameters
 */
#define MONGO_LOG_INFO_OPTIONS(ID, OPTIONS, FMTSTR_MESSAGE, ...)     \
    MONGO_LOG_IMPL(ID,                                               \
                   ::mongo::log::LogSeverity::Info(),                \
                   ::mongo::log::LogOptions::ensureValidComponent(   \
                       OPTIONS, MongologDefaultComponent_component), \
                   FMTSTR_MESSAGE,                                   \
                   ##__VA_ARGS__)

/**
 * Log with warning severity.
 *
 * See LOG() for documentation of the parameters
 */
#define MONGO_LOG_WARNING(ID, FMTSTR_MESSAGE, ...)                               \
    MONGO_LOG_IMPL(ID,                                                           \
                   ::mongo::log::LogSeverity::Warning(),                         \
                   ::mongo::log::LogOptions{MongologDefaultComponent_component}, \
                   FMTSTR_MESSAGE,                                               \
                   ##__VA_ARGS__)

/**
 * Log with warning severity and custom options.
 *
 * See LOG_OPTIONS() for documentation of the parameters
 */
#define MONGO_LOG_WARNING_OPTIONS(ID, OPTIONS, FMTSTR_MESSAGE, ...)  \
    MONGO_LOG_IMPL(ID,                                               \
                   ::mongo::log::LogSeverity::Warning(),             \
                   ::mongo::log::LogOptions::ensureValidComponent(   \
                       OPTIONS, MongologDefaultComponent_component), \
                   FMTSTR_MESSAGE,                                   \
                   ##__VA_ARGS__)

/**
 * Log with error severity.
 *
 * See LOG() for documentation of the parameters
 */
#define MONGO_LOG_ERROR(ID, FMTSTR_MESSAGE, ...)                                 \
    MONGO_LOG_IMPL(ID,                                                           \
                   ::mongo::log::LogSeverity::Error(),                           \
                   ::mongo::log::LogOptions{MongologDefaultComponent_component}, \
                   FMTSTR_MESSAGE,                                               \
                   ##__VA_ARGS__)

/**
 * Log with error severity and custom options.
 *
 * See LOG_OPTIONS() for documentation of the parameters
 */
#define MONGO_LOG_ERROR_OPTIONS(ID, OPTIONS, FMTSTR_MESSAGE, ...)    \
    MONGO_LOG_IMPL(ID,                                               \
                   ::mongo::log::LogSeverity::Error(),               \
                   ::mongo::log::LogOptions::ensureValidComponent(   \
                       OPTIONS, MongologDefaultComponent_component), \
                   FMTSTR_MESSAGE,                                   \
                   ##__VA_ARGS__)

/**
 * Log with fatal severity. fassertFailed(ID) will be performed after writing the log
 *
 * See LOG() for documentation of the parameters
 */
#define MONGO_LOG_FATAL(ID, FMTSTR_MESSAGE, ...)                                     \
    do {                                                                             \
        MONGO_LOG_IMPL(ID,                                                           \
                       ::mongo::log::LogSeverity::Severe(),                          \
                       ::mongo::log::LogOptions{MongologDefaultComponent_component}, \
                       FMTSTR_MESSAGE,                                               \
                       ##__VA_ARGS__);                                               \
        fassertFailed(ID);                                                           \
    } while (false)

/**
 * Log with fatal severity. fassertFailedNoTrace(ID) will be performed after writing the log
 *
 * See LOG() for documentation of the parameters
 */
#define MONGO_LOG_FATAL_NOTRACE(ID, FMTSTR_MESSAGE, ...)                           \
    do {                                                                           \
        MONGO_LOG_IMPL(ID,                                                         \
                       ::mongo::log::LogSeverity::Severe(),                        \
                       MAKE_OPTIONS_ARG2(MongologDefaultComponent_component,       \
                                         ::mongo::log::FatalMode::kAssertNoTrace), \
                       FMTSTR_MESSAGE,                                             \
                       ##__VA_ARGS__);                                             \
        fassertFailedNoTrace(ID);                                                  \
    } while (false)

/**
 * Log with fatal severity. Execution continues after log.
 *
 * See LOG() for documentation of the parameters
 */
#define MONGO_LOG_FATAL_CONTINUE(ID, FMTSTR_MESSAGE, ...)                                          \
    MONGO_LOG_IMPL(                                                                                \
        ID,                                                                                        \
        ::mongo::log::LogSeverity::Severe(),                                                       \
        MAKE_OPTIONS_ARG2(MongologDefaultComponent_component, ::mongo::log::FatalMode::kContinue), \
        FMTSTR_MESSAGE,                                                                            \
        ##__VA_ARGS__)

/**
 * Log with fatal severity and custom options.
 *
 * Will perform fassert after logging depending on the fatalMode() setting in OPTIONS
 *
 * See LOG_OPTIONS() for documentation of the parameters
 */
#define MONGO_LOG_FATAL_OPTIONS(ID, OPTIONS, FMTSTR_MESSAGE, ...)                 \
    do {                                                                          \
        auto optionsMacroLocal_ = ::mongo::log::LogOptions::ensureValidComponent( \
            OPTIONS, MongologDefaultComponent_component);                         \
        MONGO_LOG_IMPL(ID,                                                        \
                       ::mongo::log::LogSeverity::Severe(),                       \
                       optionsMacroLocal_,                                        \
                       FMTSTR_MESSAGE,                                            \
                       ##__VA_ARGS__);                                            \
        switch (optionsMacroLocal_.fatalMode()) {                                 \
            case ::mongo::log::FatalMode::kAssert:                                \
                fassertFailed(ID);                                                \
            case ::mongo::log::FatalMode::kAssertNoTrace:                         \
                fassertFailedNoTrace(ID);                                         \
            case ::mongo::log::FatalMode::kContinue:                              \
                break;                                                            \
        };                                                                        \
    } while (false)

/**
 * Log with debug level severity and custom options.
 *
 * DLEVEL is an integer representing the debug level. Valid range is [1, 5]
 *
 * See LOG_OPTIONS() for documentation of the other parameters
 */
#define MONGO_LOG_DEBUG_OPTIONS(ID, DLEVEL, OPTIONS, FMTSTR_MESSAGE, ...)                    \
    do {                                                                                     \
        auto severityMacroLocal_ = ::mongo::log::LogSeverity::Debug(DLEVEL);                 \
        auto optionsMacroLocal_ = ::mongo::log::LogOptions::ensureValidComponent(            \
            OPTIONS, MongologDefaultComponent_component);                                    \
        if (::mongo::log::LogManager::global().getGlobalSettings().shouldLog(                \
                optionsMacroLocal_.component(), severityMacroLocal_)) {                      \
            MONGO_LOG_IMPL(                                                                  \
                ID, severityMacroLocal_, optionsMacroLocal_, FMTSTR_MESSAGE, ##__VA_ARGS__); \
        }                                                                                    \
    } while (false)

/**
 * Log with debug level severity.
 *
 * DLEVEL is an integer representing the debug level. Valid range is [1, 5]
 *
 * See LOG() for documentation of the other parameters
 */
#define MONGO_LOG_DEBUG(ID, DLEVEL, FMTSTR_MESSAGE, ...)                                  \
    MONGO_LOG_DEBUG_OPTIONS(ID,                                                           \
                            DLEVEL,                                                       \
                            ::mongo::log::LogOptions{MongologDefaultComponent_component}, \
                            FMTSTR_MESSAGE,                                               \
                            ##__VA_ARGS__)

inline bool shouldLog(log::LogComponent logComponent, log::LogSeverity severity) {
    return log::LogManager::global().getGlobalSettings().shouldLog(logComponent, severity);
}

#ifndef REQUIRE_MONGO_LOG_PREFIX
#define LOG MONGO_LOG
#define LOG_OPTIONS MONGO_LOG_OPTIONS
#define LOG_INFO MONGO_LOG_INFO
#define LOG_INFO_OPTIONS MONGO_LOG_INFO_OPTIONS
#define LOG_WARNING MONGO_LOG_WARNING
#define LOG_WARNING_OPTIONS MONGO_LOG_WARNING_OPTIONS
#define LOG_ERROR MONGO_LOG_ERROR
#define LOG_ERROR_OPTIONS MONGO_LOG_ERROR_OPTIONS
#define LOG_FATAL MONGO_LOG_FATAL
#define LOG_FATAL_OPTIONS MONGO_LOG_FATAL_OPTIONS
#define LOG_FATAL_NOTRACE MONGO_LOG_FATAL_NOTRACE
#define LOG_FATAL_CONTINUE MONGO_LOG_FATAL_CONTINUE
#define LOG_DEBUG MONGO_LOG_DEBUG
#define LOG_DEBUG_OPTIONS MONGO_LOG_DEBUG_OPTIONS
#endif

}  // namespace mongo

#endif  // MONGO_UTIL_LOG_H_
