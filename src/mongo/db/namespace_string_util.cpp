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

#include "mongo/db/namespace_string_util.h"
//
//#include "mongo/db/ops/insert.h"
//
//#include <vector>
//
//#include "mongo/bson/bson_depth.h"
#include "mongo/db/commands/feature_compatibility_version_parser.h"
#include "mongo/db/repl/replication_coordinator.h"
//#include "mongo/db/vector_clock_mutable.h"
//#include "mongo/db/views/durable_view_catalog.h"
//#include "mongo/util/str.h"

namespace mongo {

Status userAllowedWriteNS(const NamespaceString& ns) {
    // TODO (SERVER-49545): Remove the FCV check when 5.0 becomes last-lts.
    if (ns.isSystemDotProfile() ||
        (ns.isSystemDotViews() && serverGlobalParams.featureCompatibility.isVersionInitialized() &&
         serverGlobalParams.featureCompatibility.isGreaterThanOrEqualTo(
             ServerGlobalParams::FeatureCompatibility::Version::kVersion47)) ||
        (ns.isOplog() &&
         repl::ReplicationCoordinator::get(getGlobalServiceContext())->isReplEnabled())) {
        return Status(ErrorCodes::InvalidNamespace, str::stream() << "cannot write to " << ns);
    }
    return userAllowedCreateNS(ns);
}

Status userAllowedCreateNS(const NamespaceString& ns) {
    if (!ns.isValid(NamespaceString::DollarInDbNameBehavior::Disallow)) {
        return Status(ErrorCodes::InvalidNamespace, str::stream() << "Invalid namespace: " << ns);
    }

    if (!NamespaceString::validCollectionName(ns.coll())) {
        return Status(ErrorCodes::InvalidNamespace,
                      str::stream() << "Invalid collection name: " << ns.coll());
    }

    if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer && !ns.isOnInternalDb()) {
        return Status(ErrorCodes::InvalidNamespace,
                      str::stream()
                          << "Can't create user databases on a --configsvr instance " << ns);
    }

    if (ns.isSystemDotProfile()) {
        return Status::OK();
    }

    if (ns.isSystem() && !ns.isLegalClientSystemNS()) {
        return Status(ErrorCodes::InvalidNamespace,
                      str::stream() << "Invalid system namespace: " << ns);
    }

    if (ns.isNormalCollection() && ns.size() > NamespaceString::MaxNsCollectionLen) {
        return Status(ErrorCodes::InvalidNamespace,
                      str::stream() << "Fully qualified namespace is too long. Namespace: " << ns
                                    << " Max: " << NamespaceString::MaxNsCollectionLen);
    }

    if (ns.coll().find(".system.") != std::string::npos) {
        // Writes are permitted to the persisted chunk metadata collections. These collections are
        // named based on the name of the sharded collection, e.g.
        // 'config.cache.chunks.dbname.collname'. Since there is a sharded collection
        // 'config.system.sessions', there will be a corresponding persisted chunk metadata
        // collection 'config.cache.chunks.config.system.sessions'. We wish to allow writes to this
        // collection.
        if (ns.coll().find(".system.sessions") != std::string::npos) {
            return Status::OK();
        }

        return Status(ErrorCodes::BadValue, str::stream() << "Invalid namespace: " << ns);
    }

    return Status::OK();
}
}  // namespace mongo
