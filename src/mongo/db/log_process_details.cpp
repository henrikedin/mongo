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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kControl

#include "mongo/platform/basic.h"

#include "mongo/db/log_process_details.h"

#include "mongo/db/repl/repl_set_config.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_options_server_helpers.h"
#include "mongo/util/log.h"
#include "mongo/logv2/log.h"
#include "mongo/util/net/socket_utils.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/version.h"

namespace mongo {

bool is32bit() {
    return (sizeof(int*) == 4);
}

void logProcessDetails() {
    auto&& vii = VersionInfoInterface::instance();
    LOGV2(20666, "{mongodVersion_vii}", "mongodVersion_vii"_attr = mongodVersion(vii));
    vii.logBuildInfo();

    if (ProcessInfo::getMemSizeMB() < ProcessInfo::getSystemMemSizeMB()) {
        LOGV2(20667, "{ProcessInfo_getMemSizeMB} MB of memory available to the process out of {ProcessInfo_getSystemMemSizeMB} MB total system memory", "ProcessInfo_getMemSizeMB"_attr = ProcessInfo::getMemSizeMB(), "ProcessInfo_getSystemMemSizeMB"_attr = ProcessInfo::getSystemMemSizeMB());
    }

    printCommandLineOpts();
}

void logProcessDetailsForLogRotate(ServiceContext* serviceContext) {
    LOGV2(20668, "pid={ProcessId_getCurrent} port={serverGlobalParams_port}{is32bit_32_64}-bit host={getHostNameCached}", "ProcessId_getCurrent"_attr = ProcessId::getCurrent(), "serverGlobalParams_port"_attr = serverGlobalParams.port, "is32bit_32_64"_attr = (is32bit() ? " 32" : " 64"), "getHostNameCached"_attr = getHostNameCached());

    auto replCoord = repl::ReplicationCoordinator::get(serviceContext);
    if (replCoord != nullptr &&
        replCoord->getReplicationMode() == repl::ReplicationCoordinator::modeReplSet) {
        auto rsConfig = replCoord->getConfig();

        if (rsConfig.isInitialized()) {
            LOGV2(20669, "Replica Set Config: {rsConfig_toBSON}", "rsConfig_toBSON"_attr = rsConfig.toBSON());
            LOGV2(20670, "Replica Set Member State: {replCoord_getMemberState_toString}", "replCoord_getMemberState_toString"_attr = (replCoord->getMemberState()).toString());
        } else {
            LOGV2(20671, "Node currently has no Replica Set Config.");
        }
    }

    logProcessDetails();
}

}  // namespace mongo
