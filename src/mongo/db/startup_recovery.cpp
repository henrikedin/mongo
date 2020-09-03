/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "startup_recovery.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/create_collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/multi_index_block.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/commands/feature_compatibility_version_document_gen.h"
#include "mongo/db/commands/feature_compatibility_version_documentation.h"
#include "mongo/db/commands/feature_compatibility_version_parser.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index_builds_coordinator.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/rebuild_indexes.h"
#include "mongo/db/repair.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/repl_set_member_in_standalone_mode.h"
#include "mongo/db/server_options.h"
#include "mongo/db/storage/durable_catalog.h"
#include "mongo/db/storage/storage_repair_observer.h"
#include "mongo/logv2/log.h"
#include "mongo/util/exit.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/quick_exit.h"

#if !defined(_WIN32)
#include <sys/file.h>
#endif

namespace mongo {

// Exit after repair has started, but before data is repaired.
MONGO_FAIL_POINT_DEFINE(exitBeforeDataRepair);
// Exit after repairing data, but before the replica set configuration is invalidated.
MONGO_FAIL_POINT_DEFINE(exitBeforeRepairInvalidatesConfig);

namespace {

// Returns true if storage engine is writable.
bool isWriteableStorageEngine() {
    return !storageGlobalParams.readOnly && (storageGlobalParams.engine != "devnull");
}

// Attempt to restore the featureCompatibilityVersion document if it is missing.
Status restoreMissingFeatureCompatibilityVersionDocument(OperationContext* opCtx) {
    NamespaceString fcvNss(NamespaceString::kServerConfigurationNamespace);

    // If the admin database, which contains the server configuration collection with the
    // featureCompatibilityVersion document, does not exist, create it.
    auto databaseHolder = DatabaseHolder::get(opCtx);
    auto db = databaseHolder->getDb(opCtx, fcvNss.db());
    if (!db) {
        LOGV2(20998, "Re-creating admin database that was dropped.");
    }
    db = databaseHolder->openDb(opCtx, fcvNss.db());
    invariant(db);

    // If the server configuration collection, which contains the FCV document, does not exist, then
    // create it.
    if (!CollectionCatalog::get(opCtx).lookupCollectionByNamespace(
            opCtx, NamespaceString::kServerConfigurationNamespace)) {
        // (Generic FCV reference): This FCV reference should exist across LTS binary versions.
        LOGV2(4926905,
              "Re-creating featureCompatibilityVersion document that was deleted. Creating new "
              "document with last LTS version.",
              "version"_attr = FeatureCompatibilityVersionParser::kLastLTS);
        uassertStatusOK(
            createCollection(opCtx, fcvNss.db().toString(), BSON("create" << fcvNss.coll())));
    }

    const Collection* fcvColl = CollectionCatalog::get(opCtx).lookupCollectionByNamespace(
        opCtx, NamespaceString::kServerConfigurationNamespace);
    invariant(fcvColl);

    // Restore the featureCompatibilityVersion document if it is missing.
    BSONObj featureCompatibilityVersion;
    if (!Helpers::findOne(opCtx,
                          fcvColl,
                          BSON("_id" << FeatureCompatibilityVersionParser::kParameterName),
                          featureCompatibilityVersion)) {
        LOGV2(21000,
              "Re-creating featureCompatibilityVersion document that was deleted. Creating new "
              "document with version "
              "{FeatureCompatibilityVersionParser_kVersion44}.",
              "Re-creating featureCompatibilityVersion document that was deleted",
              "version"_attr = FeatureCompatibilityVersionParser::kVersion44);

        FeatureCompatibilityVersionDocument fcvDoc;
        // (Generic FCV reference): This FCV reference should exist across LTS binary versions.
        fcvDoc.setVersion(ServerGlobalParams::FeatureCompatibility::kLastLTS);

        writeConflictRetry(opCtx, "insertFCVDocument", fcvNss.ns(), [&] {
            WriteUnitOfWork wunit(opCtx);
            OpDebug* const nullOpDebug = nullptr;
            uassertStatusOK(fcvColl->insertDocument(
                opCtx, InsertStatement(fcvDoc.toBSON()), nullOpDebug, false));
            wunit.commit();
        });
    }

    invariant(Helpers::findOne(opCtx,
                               fcvColl,
                               BSON("_id" << FeatureCompatibilityVersionParser::kParameterName),
                               featureCompatibilityVersion));

    return Status::OK();
}

/**
 * Returns true if the collection associated with the given CollectionCatalogEntry has an index on
 * the _id field
 */
bool checkIdIndexExists(OperationContext* opCtx, RecordId catalogId) {
    auto durableCatalog = DurableCatalog::get(opCtx);
    auto indexCount = durableCatalog->getTotalIndexCount(opCtx, catalogId);
    auto indexNames = std::vector<std::string>(indexCount);
    durableCatalog->getAllIndexes(opCtx, catalogId, &indexNames);

    for (auto name : indexNames) {
        if (name == "_id_") {
            return true;
        }
    }
    return false;
}

Status buildMissingIdIndex(OperationContext* opCtx, Collection* collection) {
    LOGV2(4805002, "Building missing _id index", logAttrs(*collection));
    MultiIndexBlock indexer;
    auto abortOnExit = makeGuard(
        [&] { indexer.abortIndexBuild(opCtx, collection, MultiIndexBlock::kNoopOnCleanUpFn); });

    const auto indexCatalog = collection->getIndexCatalog();
    const auto idIndexSpec = indexCatalog->getDefaultIdIndexSpec();

    auto swSpecs = indexer.init(opCtx, collection, idIndexSpec, MultiIndexBlock::kNoopOnInitFn);
    if (!swSpecs.isOK()) {
        return swSpecs.getStatus();
    }

    auto status = indexer.insertAllDocumentsInCollection(opCtx, collection);
    if (!status.isOK()) {
        return status;
    }

    status = indexer.checkConstraints(opCtx);
    if (!status.isOK()) {
        return status;
    }

    WriteUnitOfWork wuow(opCtx);
    status = indexer.commit(
        opCtx, collection, MultiIndexBlock::kNoopOnCreateEachFn, MultiIndexBlock::kNoopOnCommitFn);
    wuow.commit();
    abortOnExit.dismiss();
    return status;
}

auto downgradeError =
    Status{ErrorCodes::MustDowngrade,
           str::stream() << "UPGRADE PROBLEM: The data files need to be fully upgraded to version "
                            "4.4 before attempting a binary upgrade; see "
                         << feature_compatibility_version_documentation::kUpgradeLink
                         << " for more details."};

/**
 * Checks that all collections on a database have valid properties for this version of MongoDB.
 *
 * This validates that required collections have an _id index. If a collection is missing an _id
 * index, this function will build it if EnsureIndexPolicy is kBuildMissing.
 *
 * Returns a MustDowngrade error if any index builds on the required _id field fail.
 */
enum class EnsureIndexPolicy { kBuildMissing, kError };
Status ensureCollectionProperties(OperationContext* opCtx,
                                  Database* db,
                                  EnsureIndexPolicy ensureIndexPolicy) {
    for (auto collIt = db->begin(opCtx); collIt != db->end(opCtx); ++collIt) {
        auto coll = collIt.getWritableCollection(opCtx);
        if (!coll) {
            break;
        }

        // All user-created replicated collections created since MongoDB 4.0 have _id indexes.
        auto requiresIndex = coll->requiresIdIndex() && coll->ns().isReplicated();
        auto collOptions =
            DurableCatalog::get(opCtx)->getCollectionOptions(opCtx, coll->getCatalogId());
        auto hasAutoIndexIdField = collOptions.autoIndexId == CollectionOptions::YES;

        // Even if the autoIndexId field is not YES, the collection may still have an _id index
        // that was created manually by the user. Check the list of indexes to confirm index
        // does not exist before attempting to build it or returning an error.
        if (requiresIndex && !hasAutoIndexIdField &&
            !checkIdIndexExists(opCtx, coll->getCatalogId())) {
            LOGV2(21001,
                  "collection {coll_ns} is missing an _id index",
                  "Collection is missing an _id index",
                  logAttrs(*coll));
            if (EnsureIndexPolicy::kBuildMissing == ensureIndexPolicy) {
                auto status = buildMissingIdIndex(opCtx, coll);
                if (!status.isOK()) {
                    LOGV2_ERROR(21021,
                                "could not build an _id index on collection {coll_ns}: {error}",
                                "Could not build an _id index on collection",
                                logAttrs(*coll),
                                "error"_attr = status);
                    return downgradeError;
                }
            } else {
                return downgradeError;
            }
        }
    }
    return Status::OK();
}

/**
 * Opens each database and provides a callback on each one.
 */
template <typename Func>
void openDatabases(OperationContext* opCtx, const StorageEngine* storageEngine, Func&& onDatabase) {
    invariant(opCtx->lockState()->isW());

    auto databaseHolder = DatabaseHolder::get(opCtx);
    auto dbNames = storageEngine->listDatabases();
    for (const auto& dbName : dbNames) {
        LOGV2_DEBUG(21010, 1, "    Opening database: {dbName}", "dbName"_attr = dbName);
        auto db = databaseHolder->openDb(opCtx, dbName);
        invariant(db);

        onDatabase(db);
    }
}

// Check for storage engine file compatibility. Exits the process if there is an incompatibility.
void assertFilesCompatible(OperationContext* opCtx, StorageEngine* storageEngine) {
    auto status = storageEngine->currentFilesCompatible(opCtx);
    if (status.isOK()) {
        return;
    }

    if (status.code() == ErrorCodes::CanRepairToDowngrade) {
        // Convert CanRepairToDowngrade statuses to MustUpgrade statuses to avoid logging a
        // potentially confusing and inaccurate message.
        //
        // TODO SERVER-24097: Log a message informing the user that they can start the current
        // version of mongod with --repair and then proceed with normal startup.
        status = {ErrorCodes::MustUpgrade, status.reason()};
    }
    LOGV2_FATAL_CONTINUE(
        21023,
        "Unable to start mongod due to an incompatibility with the data files and this version "
        "of mongod: {error}. Please consult our documentation when trying to downgrade to a "
        "previous major release",
        "Unable to start mongod due to an incompatibility with the data files and this version "
        "of mongod. Please consult our documentation when trying to downgrade to a previous "
        "major release",
        "error"_attr = redact(status));
    quickExit(EXIT_NEED_UPGRADE);
    MONGO_UNREACHABLE;
}


/**
 * Returns 'true' if this server has a configuration document in local.system.replset.
 */
bool hasReplSetConfigDoc(OperationContext* opCtx) {
    auto databaseHolder = DatabaseHolder::get(opCtx);

    // We open the "local" database before reading to ensure the in-memory catalog entries for the
    // 'kSystemReplSetNamespace' collection have been populated if the collection exists. If the
    // "local" database doesn't exist at this point yet, then it will be created.
    const auto nss = NamespaceString::kSystemReplSetNamespace;
    databaseHolder->openDb(opCtx, nss.db());
    BSONObj config;
    return Helpers::getSingleton(opCtx, nss.ns().c_str(), config);
}

/**
 * Check that the oplog is capped, and abort the process if it is not.
 * Caller must lock DB before calling this function.
 */
void assertCappedOplog(OperationContext* opCtx, Database* db) {
    const NamespaceString oplogNss(NamespaceString::kRsOplogNamespace);
    invariant(opCtx->lockState()->isDbLockedForMode(oplogNss.db(), MODE_IS));
    const Collection* oplogCollection =
        CollectionCatalog::get(opCtx).lookupCollectionByNamespace(opCtx, oplogNss);
    if (oplogCollection && !oplogCollection->isCapped()) {
        LOGV2_FATAL_NOTRACE(
            40115,
            "The oplog collection {oplogNamespace} is not capped; a capped oplog is a "
            "requirement for replication to function.",
            "The oplog collection is not capped; a capped oplog is a "
            "requirement for replication to function.",
            "oplogNamespace"_attr = oplogNss);
    }
}

void reconcileCatalogAndRebuildUnfinishedIndexes(
    OperationContext* opCtx,
    StorageEngine* storageEngine,
    LastStorageEngineShutdownState lastStorageEngineShutdownState) {

    // When starting up after an unclean shutdown, we do not attempt to recover any state from the
    // internal idents. Thus, we drop them in this case.
    auto reconcilePolicy =
        LastStorageEngineShutdownState::kUnclean == lastStorageEngineShutdownState
        ? StorageEngine::InternalIdentReconcilePolicy::kDrop
        : StorageEngine::InternalIdentReconcilePolicy::kRetain;
    auto reconcileResult =
        fassert(40593, storageEngine->reconcileCatalogAndIdents(opCtx, reconcilePolicy));

    // If we did not find any index builds to resume or we are starting up after an unclean
    // shutdown, nothing in the temp directory will be used. Thus, we can clear it.
    if (reconcileResult.indexBuildsToResume.empty() ||
        lastStorageEngineShutdownState == LastStorageEngineShutdownState::kUnclean) {
        LOGV2(5071100, "Clearing temp directory");

        boost::system::error_code ec;
        boost::filesystem::remove_all(storageGlobalParams.dbpath + "/_tmp/", ec);

        if (ec) {
            LOGV2(5071101, "Failed to clear temp directory", "error"_attr = ec.message());
        }
    }

    // Determine which indexes need to be rebuilt. rebuildIndexesOnCollection() requires that all
    // indexes on that collection are done at once, so we use a map to group them together.
    StringMap<IndexNameObjs> nsToIndexNameObjMap;
    for (auto&& idxIdentifier : reconcileResult.indexesToRebuild) {
        NamespaceString collNss = idxIdentifier.nss;
        const std::string& indexName = idxIdentifier.indexName;
        auto swIndexSpecs =
            getIndexNameObjs(opCtx, idxIdentifier.catalogId, [&indexName](const std::string& name) {
                return name == indexName;
            });
        if (!swIndexSpecs.isOK() || swIndexSpecs.getValue().first.empty()) {
            fassert(40590,
                    {ErrorCodes::InternalError,
                     str::stream() << "failed to get index spec for index " << indexName
                                   << " in collection " << collNss.toString()});
        }

        auto& indexesToRebuild = swIndexSpecs.getValue();
        invariant(indexesToRebuild.first.size() == 1 && indexesToRebuild.second.size() == 1,
                  str::stream() << "Num Index Names: " << indexesToRebuild.first.size()
                                << " Num Index Objects: " << indexesToRebuild.second.size());
        auto& ino = nsToIndexNameObjMap[collNss.ns()];
        ino.first.emplace_back(std::move(indexesToRebuild.first.back()));
        ino.second.emplace_back(std::move(indexesToRebuild.second.back()));
    }

    for (const auto& entry : nsToIndexNameObjMap) {
        NamespaceString collNss(entry.first);

        auto collection = CollectionCatalog::get(opCtx).lookupCollectionByNamespace(opCtx, collNss);
        for (const auto& indexName : entry.second.first) {
            LOGV2(21004,
                  "Rebuilding index. Collection: {collNss} Index: {indexName}",
                  "Rebuilding index",
                  "namespace"_attr = collNss,
                  "index"_attr = indexName);
        }

        std::vector<BSONObj> indexSpecs = entry.second.second;
        fassert(40592, rebuildIndexesOnCollection(opCtx, collection, indexSpecs, RepairData::kNo));
    }

    // Two-phase index builds depend on an eventually-replicated 'commitIndexBuild' oplog entry to
    // complete. Therefore, when a replica set member is started in standalone mode, we cannot
    // restart the index build because it will never complete.
    if (getReplSetMemberInStandaloneMode(opCtx->getServiceContext())) {
        LOGV2(21005, "Not restarting unfinished index builds because we are in standalone mode");
        return;
    }

    // Once all unfinished indexes have been rebuilt, restart any unfinished index builds. This will
    // not build any indexes to completion, but rather start the background thread to build the
    // index, and wait for a replicated commit or abort oplog entry.
    IndexBuildsCoordinator::get(opCtx)->restartIndexBuildsForRecovery(
        opCtx, reconcileResult.indexBuildsToRestart, reconcileResult.indexBuildsToResume);
}

/**
 * Sets the appropriate flag on the service context decorable 'replSetMemberInStandaloneMode' to
 * 'true' if this is a replica set node running in standalone mode, otherwise 'false'.
 */
void setReplSetMemberInStandaloneMode(OperationContext* opCtx) {
    const repl::ReplSettings& replSettings =
        repl::ReplicationCoordinator::get(opCtx)->getSettings();

    if (replSettings.usingReplSets()) {
        // Not in standalone mode.
        setReplSetMemberInStandaloneMode(opCtx->getServiceContext(), false);
        return;
    }

    invariant(opCtx->lockState()->isW());
    const Collection* collection = CollectionCatalog::get(opCtx).lookupCollectionByNamespace(
        opCtx, NamespaceString::kSystemReplSetNamespace);
    if (collection && !collection->isEmpty(opCtx)) {
        setReplSetMemberInStandaloneMode(opCtx->getServiceContext(), true);
        return;
    }

    setReplSetMemberInStandaloneMode(opCtx->getServiceContext(), false);
}

// Perform startup procedures for --repair mode.
void startupRepair(OperationContext* opCtx, StorageEngine* storageEngine) {
    invariant(!storageGlobalParams.readOnly);

    if (MONGO_unlikely(exitBeforeDataRepair.shouldFail())) {
        LOGV2(21006, "Exiting because 'exitBeforeDataRepair' fail point was set.");
        quickExit(EXIT_ABRUPT);
    }

    // Repair, restore, and initialize the featureCompatibilityVersion document before allowing
    // repair to potentially rebuild indexes on the remaining collections. This ensures any
    // FCV-dependent features are rebuilt properly. Note that we don't try to prevent
    // repairDatabase from repairing this collection again, because it only consists of one
    // document.
    // If we fail to load the FCV document due to upgrade problems, we need to abort the repair in
    // order to allow downgrading to older binary versions.
    auto abortRepairOnFCVErrors = makeGuard(
        [&] { StorageRepairObserver::get(opCtx->getServiceContext())->onRepairDone(opCtx); });
    if (auto fcvColl = CollectionCatalog::get(opCtx).lookupCollectionByNamespace(
            opCtx, NamespaceString::kServerConfigurationNamespace)) {
        auto databaseHolder = DatabaseHolder::get(opCtx);
        databaseHolder->openDb(opCtx, fcvColl->ns().db());
        fassertNoTrace(4805000,
                       repair::repairCollection(
                           opCtx, storageEngine, NamespaceString::kServerConfigurationNamespace));
    }
    uassertStatusOK(restoreMissingFeatureCompatibilityVersionDocument(opCtx));
    FeatureCompatibilityVersion::initializeForStartup(opCtx);
    abortRepairOnFCVErrors.dismiss();

    // The local database should be repaired before any other replicated collections so we know
    // whether not to rebuild unfinished two-phase index builds if this is a replica set node
    // running in standalone mode.
    auto dbNames = storageEngine->listDatabases();
    if (auto it = std::find(dbNames.begin(), dbNames.end(), NamespaceString::kLocalDb);
        it != dbNames.end()) {
        fassertNoTrace(4805001, repair::repairDatabase(opCtx, storageEngine, *it));

        // This must be set before rebuilding index builds on replicated collections.
        setReplSetMemberInStandaloneMode(opCtx);
        dbNames.erase(it);
    }

    // Repair the remaining databases.
    for (const auto& dbName : dbNames) {
        fassertNoTrace(18506, repair::repairDatabase(opCtx, storageEngine, dbName));
    }

    openDatabases(opCtx, storageEngine, [&](auto db) {
        // Ensures all collections meet requirements such as having _id indexes, and corrects them
        // if needed.
        uassertStatusOK(ensureCollectionProperties(opCtx, db, EnsureIndexPolicy::kBuildMissing));
    });

    if (MONGO_unlikely(exitBeforeRepairInvalidatesConfig.shouldFail())) {
        LOGV2(21008, "Exiting because 'exitBeforeRepairInvalidatesConfig' fail point was set.");
        quickExit(EXIT_ABRUPT);
    }

    auto repairObserver = StorageRepairObserver::get(opCtx->getServiceContext());
    repairObserver->onRepairDone(opCtx);
    if (repairObserver->getModifications().size() > 0) {
        const auto& mods = repairObserver->getModifications();
        for (const auto& mod : mods) {
            LOGV2_WARNING(21019, "repairModification", "description"_attr = mod.getDescription());
        }
    }
    if (repairObserver->isDataInvalidated()) {
        if (hasReplSetConfigDoc(opCtx)) {
            LOGV2_WARNING(21020,
                          "WARNING: Repair may have modified replicated data. This node will no "
                          "longer be able to join a replica set without a full re-sync");
        }
    }

    // There were modifications, but only benign ones.
    if (repairObserver->getModifications().size() > 0 && !repairObserver->isDataInvalidated()) {
        LOGV2(21009,
              "Repair has made modifications to unreplicated data. The data is healthy and "
              "the node is eligible to be returned to the replica set.");
    }
}

// Perform startup procedures for read-only mode.
void startupRecoveryReadOnly(OperationContext* opCtx, StorageEngine* storageEngine) {
    invariant(!storageGlobalParams.repair);

    setReplSetMemberInStandaloneMode(opCtx);

    FeatureCompatibilityVersion::initializeForStartup(opCtx);

    openDatabases(opCtx, storageEngine, [&](auto db) {
        // Ensures all collections meet requirements such as having _id indexes.
        uassertStatusOK(ensureCollectionProperties(opCtx, db, EnsureIndexPolicy::kError));
    });
}

// Perform routine startup recovery procedure.
void startupRecovery(OperationContext* opCtx,
                     StorageEngine* storageEngine,
                     LastStorageEngineShutdownState lastStorageEngineShutdownState) {
    invariant(!storageGlobalParams.readOnly && !storageGlobalParams.repair);

    // Determine whether this is a replica set node running in standalone mode. This must be set
    // before determining whether to restart index builds.
    setReplSetMemberInStandaloneMode(opCtx);

    // Initialize FCV before rebuilding indexes that may have features dependent on FCV.
    FeatureCompatibilityVersion::initializeForStartup(opCtx);

    // Drops abandoned idents. Rebuilds unfinished indexes and restarts incomplete two-phase
    // index builds.
    reconcileCatalogAndRebuildUnfinishedIndexes(
        opCtx, storageEngine, lastStorageEngineShutdownState);

    const auto& replSettings = repl::ReplicationCoordinator::get(opCtx)->getSettings();

    // On replica set members we only clear temp collections on DBs other than "local" during
    // promotion to primary. On secondaries, they are only cleared when the oplog tells them to. The
    // local DB is special because it is not replicated.  See SERVER-10927 for more details.
    const bool shouldClearNonLocalTmpCollections =
        !(hasReplSetConfigDoc(opCtx) || replSettings.usingReplSets());

    openDatabases(opCtx, storageEngine, [&](auto db) {
        auto dbName = db->name();

        // Ensures all collections meet requirements such as having _id indexes, and corrects them
        // if needed.
        uassertStatusOK(ensureCollectionProperties(opCtx, db, EnsureIndexPolicy::kBuildMissing));

        if (replSettings.usingReplSets()) {
            // We only care about _id indexes and drop-pending collections if we are in a replset.
            db->checkForIdIndexesAndDropPendingCollections(opCtx);
            // Ensure oplog is capped (mongodb does not guarantee order of inserts on noncapped
            // collections)
            if (db->name() == NamespaceString::kLocalDb) {
                assertCappedOplog(opCtx, db);
            }
        }

        if (shouldClearNonLocalTmpCollections || dbName == NamespaceString::kLocalDb) {
            db->clearTmpCollections(opCtx);
        }
    });
}

}  // namespace

namespace startup_recovery {
/**
 * Recovers or repairs all databases from a previous shutdown. May throw a MustDowngrade error
 * if data files are incompatible with the current binary version.
 */
void repairAndRecoverDatabases(OperationContext* opCtx,
                               LastStorageEngineShutdownState lastStorageEngineShutdownState) {
    auto const storageEngine = opCtx->getServiceContext()->getStorageEngine();
    Lock::GlobalWrite lk(opCtx);

    // Create the FCV document for the first time, if necessary. Replica set nodes only initialize
    // the FCV when the replica set is first initiated or by data replication.
    const auto& replSettings = repl::ReplicationCoordinator::get(opCtx)->getSettings();
    if (isWriteableStorageEngine() && !replSettings.usingReplSets()) {
        FeatureCompatibilityVersion::setIfCleanStartup(opCtx, repl::StorageInterface::get(opCtx));
    }

    if (storageGlobalParams.repair) {
        startupRepair(opCtx, storageEngine);
    } else if (storageGlobalParams.readOnly) {
        startupRecoveryReadOnly(opCtx, storageEngine);
    } else {
        startupRecovery(opCtx, storageEngine, lastStorageEngineShutdownState);
    }

    assertFilesCompatible(opCtx, storageEngine);
}
}  // namespace startup_recovery
}  // namespace mongo
