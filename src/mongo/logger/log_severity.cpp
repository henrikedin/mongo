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

#include "mongo/logger/log_severity.h"

#include <iostream>

namespace mongo {
namespace logger {

namespace {
constexpr auto unknownSeverityString = "UNKNOWN"_sd;
constexpr auto severeSeverityString = "SEVERE"_sd;
constexpr auto errorSeverityString = "ERROR"_sd;
constexpr auto warningSeverityString = "warning"_sd;
constexpr auto infoSeverityString = "info"_sd;
constexpr auto debug1SeverityString = "debug1"_sd;
constexpr auto debug2SeverityString = "debug2"_sd;
constexpr auto debug3SeverityString = "debug3"_sd;
constexpr auto debug4SeverityString = "debug4"_sd;
constexpr auto debug5SeverityString = "debug5"_sd;
constexpr auto debug6SeverityString = "debug6"_sd;
constexpr auto debug7SeverityString = "debug7"_sd;
constexpr auto debug8SeverityString = "debug8"_sd;
constexpr auto debug9SeverityString = "debug9"_sd;
}  // namespace

StringData LogSeverity::toStringData() const {
    if (_severity == 1)
        return debug1SeverityString;
    if (_severity == 2)
        return debug2SeverityString;
    if (_severity == 3)
        return debug3SeverityString;
    if (_severity == 4)
        return debug4SeverityString;
    if (_severity == 5)
        return debug5SeverityString;
    if (_severity == 6)
        return debug6SeverityString;
    if (_severity == 7)
        return debug7SeverityString;
    if (_severity == 8)
        return debug8SeverityString;
    if (_severity == 9)
        return debug9SeverityString;
    if (*this == LogSeverity::Severe())
        return severeSeverityString;
    if (*this == LogSeverity::Error())
        return errorSeverityString;
    if (*this == LogSeverity::Warning())
        return warningSeverityString;
    if (*this == LogSeverity::Info())
        return infoSeverityString;
    if (*this == LogSeverity::Log())
        return infoSeverityString;
    return unknownSeverityString;
}

char LogSeverity::toChar() const {
    if (_severity == 1)
        return '1';
    if (_severity == 2)
        return '2';
    if (_severity == 3)
        return '3';
    if (_severity == 4)
        return '4';
    if (_severity == 5)
        return '5';
    if (_severity == 6)
        return '6';
    if (_severity == 7)
        return '7';
    if (_severity == 8)
        return '8';
    if (_severity == 9)
        return '9';
    // 'S' might be confused with "Success"
    // Return 'F' to imply Fatal instead.
    if (*this == LogSeverity::Severe())
        return 'F';
    if (*this == LogSeverity::Error())
        return 'E';
    if (*this == LogSeverity::Warning())
        return 'W';
    if (*this == LogSeverity::Info())
        return 'I';
    if (*this == LogSeverity::Log())
        return 'I';
    // Should not reach here - returning 'U' for Unknown severity
    // to be consistent with toStringData().
    return 'U';
}

std::ostream& operator<<(std::ostream& os, LogSeverity severity) {
    return os << severity.toStringData();
}

}  // namespace logger
}  // namespace mongo
