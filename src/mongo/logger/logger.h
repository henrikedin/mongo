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

#pragma once

#include "mongo/logger/log_manager.h"
#include "mongo/logger/message_log_domain.h"
#include "mongo/logger/rotatable_file_manager.h"

namespace mongo {
namespace logger {

/**
 * Gets a global singleton instance of RotatableFileManager.
 */
RotatableFileManager* globalRotatableFileManager();

/**
 * Gets a global singleton instance of LogManager.
 */
LogManager* globalLogManager();

/**
 * Gets the global MessageLogDomain associated for the global log manager.
 */
inline ComponentMessageLogDomain* globalLogDomain() {
    return globalLogManager()->getGlobalDomain();
}

}  // namespace logger
}  // namespace mongo

#define MONGO_BOOST_LOG_COMPONENT(component) BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), component, mongo::logger::LogSeverity::Info())
#define MONGO_BOOST_WARNING_COMPONENT(component) BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), component, mongo::logger::LogSeverity::Warning())
#define MONGO_BOOST_ERROR_COMPONENT(component) BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), component, mongo::logger::LogSeverity::Error())
#define MONGO_BOOST_SEVERE_COMPONENT(component) BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), component, mongo::logger::LogSeverity::Severe())

#define MONGO_BOOST_LOG BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), ::MongoLogDefaultComponent_component, mongo::logger::LogSeverity::Info())
#define MONGO_BOOST_WARNING BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), ::MongoLogDefaultComponent_component, mongo::logger::LogSeverity::Warning())
#define MONGO_BOOST_ERROR BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), ::MongoLogDefaultComponent_component, mongo::logger::LogSeverity::Error())
#define MONGO_BOOST_SEVERE BOOST_LOG_STREAM_CHANNEL_SEV(mongo::logger::globalLogManager()->getGlobalLogger(), ::MongoLogDefaultComponent_component, mongo::logger::LogSeverity::Severe())