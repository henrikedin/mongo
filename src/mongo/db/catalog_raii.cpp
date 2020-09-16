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

#include "mongo/db/catalog_raii.h"

#include "mongo/db/catalog/collection_catalog.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/namespace_string_util.h"
#include "mongo/db/s/database_sharding_state.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/util/fail_point.h"

namespace mongo {
namespace {

MONGO_FAIL_POINT_DEFINE(setAutoGetCollectionWait);

}  // namespace

AutoGetDb::AutoGetDb(OperationContext* opCtx, StringData dbName, LockMode mode, Date_t deadline)
    : _opCtx(opCtx), _dbName(dbName), _dbLock(opCtx, dbName, mode, deadline), _db([&] {
          auto databaseHolder = DatabaseHolder::get(opCtx);
          return databaseHolder->getDb(opCtx, dbName);
      }()) {
    auto dss = DatabaseShardingState::get(opCtx, dbName);
    auto dssLock = DatabaseShardingState::DSSLock::lockShared(opCtx, dss);
    dss->checkDbVersion(opCtx, dssLock);
}

Database* AutoGetDb::ensureDbExists() {
    if (_db) {
        return _db;
    }

    auto databaseHolder = DatabaseHolder::get(_opCtx);
    _db = databaseHolder->openDb(_opCtx, _dbName, nullptr);

    auto dss = DatabaseShardingState::get(_opCtx, _dbName);
    auto dssLock = DatabaseShardingState::DSSLock::lockShared(_opCtx, dss);
    dss->checkDbVersion(_opCtx, dssLock);

    return _db;
}

template <typename CatalogCollectionLookupT>
AutoGetCollectionBase<CatalogCollectionLookupT>::AutoGetCollectionBase(
    OperationContext* opCtx,
    const NamespaceStringOrUUID& nsOrUUID,
    LockMode modeColl,
    AutoGetCollectionViewMode viewMode,
    Date_t deadline,
    AutoGetCollectionEnsureMode ensureMode)
    : _autoDb(opCtx,
              !nsOrUUID.dbname().empty() ? nsOrUUID.dbname() : nsOrUUID.nss()->db(),
              isSharedLockMode(modeColl) ? MODE_IS : MODE_IX,
              deadline) {
    // EnsureExists may not be used in a WUOW and it requires at least MODE_IX
    if (ensureMode == AutoGetCollectionEnsureMode::kEnsureExists) {
        invariant(!opCtx->lockState()->inAWriteUnitOfWork());
        invariant(modeColl == MODE_IX || modeColl == MODE_X);
    }
    
    if (auto& nss = nsOrUUID.nss()) {
        uassert(ErrorCodes::InvalidNamespace,
                str::stream() << "Namespace " << *nss << " is not a valid collection name",
                nss->isValid());
    }

    _collLock.emplace(opCtx, nsOrUUID, modeColl, deadline);
    _resolvedNss = CollectionCatalog::get(opCtx).resolveNamespaceStringOrUUID(opCtx, nsOrUUID);

    // Wait for a configured amount of time after acquiring locks if the failpoint is enabled
    setAutoGetCollectionWait.execute(
        [&](const BSONObj& data) { sleepFor(Milliseconds(data["waitForMillis"].numberInt())); });

    Database* const db = _autoDb.getDb();
    invariant(!nsOrUUID.uuid() || db,
              str::stream() << "Database for " << _resolvedNss.ns()
                            << " disappeared after successufully resolving "
                            << nsOrUUID.toString());

    // In most cases we expect modifications for system.views to upgrade MODE_IX to MODE_X before
    // taking the lock. One exception is a query by UUID of system.views in a transaction. Usual
    // queries of system.views (by name, not UUID) within a transaction are rejected. However, if
    // the query is by UUID we can't determine whether the namespace is actually system.views until
    // we take the lock here. So we have this one last assertion.
    uassert(51070,
            "Modifications to system.views must take an exclusive lock",
            !_resolvedNss.isSystemDotViews() || modeColl != MODE_IX);

    if (ensureMode == AutoGetCollectionEnsureMode::kEnsureExists) {
        _autoDb.ensureDbExists();
    }

    // If the database doesn't exists, we can't obtain a collection or check for views
    if (!db)
        return;

    _coll = CatalogCollectionLookupT::lookupCollection(opCtx, _resolvedNss);
    invariant(!nsOrUUID.uuid() || _coll,
              str::stream() << "Collection for " << _resolvedNss.ns()
                            << " disappeared after successufully resolving "
                            << nsOrUUID.toString());

    if (!_coll && ensureMode == AutoGetCollectionEnsureMode::kEnsureExists) {
        uassertStatusOK(userAllowedCreateNS(_resolvedNss));
        uassert(ErrorCodes::PrimarySteppedDown,
            str::stream() << "Not primary while writing to " << _resolvedNss.ns(),
            repl::ReplicationCoordinator::get(opCtx->getServiceContext())
                ->canAcceptWritesFor(opCtx, _resolvedNss));
        CollectionShardingState::get(opCtx, _resolvedNss)->checkShardVersionOrThrow(opCtx);
        writeConflictRetry(
            opCtx, "AutoGetCollection ensure collection exists", _resolvedNss.ns(), [this, &opCtx] {
                WriteUnitOfWork wuow(opCtx);
                CollectionOptions defaultCollectionOptions;
                uassertStatusOK(
                    _autoDb.getDb()->userCreateNS(opCtx, _resolvedNss, defaultCollectionOptions));
                wuow.commit();

                _coll = CatalogCollectionLookupT::lookupCollection(opCtx, _resolvedNss);
            });
    }

    if (_coll) {
        // If we are in a transaction, we cannot yield and wait when there are pending catalog
        // changes. Instead, we must return an error in such situations. We
        // ignore this restriction for the oplog, since it never has pending catalog changes.
        if (opCtx->inMultiDocumentTransaction() &&
            _resolvedNss != NamespaceString::kRsOplogNamespace) {

            if (auto minSnapshot = _coll->getMinimumVisibleSnapshot()) {
                auto mySnapshot = opCtx->recoveryUnit()->getPointInTimeReadTimestamp().get_value_or(
                    opCtx->recoveryUnit()->getCatalogConflictingTimestamp());

                uassert(ErrorCodes::SnapshotUnavailable,
                        str::stream()
                            << "Unable to read from a snapshot due to pending collection catalog "
                               "changes; please retry the operation. Snapshot timestamp is "
                            << mySnapshot.toString() << ". Collection minimum is "
                            << minSnapshot->toString(),
                        mySnapshot.isNull() || mySnapshot >= minSnapshot.get());
            }
        }

        // If the collection exists, there is no need to check for views.
        return;
    }

    _view = ViewCatalog::get(db)->lookup(opCtx, _resolvedNss.ns());
    uassert(ErrorCodes::CommandNotSupportedOnView,
            str::stream() << "Namespace " << _resolvedNss.ns() << " is a view, not a collection",
            !_view || viewMode == AutoGetCollectionViewMode::kViewsPermitted);
}

AutoGetCollection::AutoGetCollection(OperationContext* opCtx,
                                     const NamespaceStringOrUUID& nsOrUUID,
                                     LockMode modeColl,
                                     AutoGetCollectionViewMode viewMode,
                                     Date_t deadline,
                                     AutoGetCollectionEnsureMode ensureMode)
    : AutoGetCollectionBase(opCtx, nsOrUUID, modeColl, viewMode, deadline, ensureMode),
      _opCtx(opCtx) {}

Collection* AutoGetCollection::getWritableCollection(CollectionCatalog::LifetimeMode mode) {
    // Acquire writable instance if not already available
    if (!_writableColl) {

        // Resets the writable Collection when the write unit of work finishes so we re-fetches and
        // re-clones the Collection if a new write unit of work is opened.
        class WritableCollectionReset : public RecoveryUnit::Change {
        public:
            WritableCollectionReset(AutoGetCollection& autoColl,
                                    const Collection* rollbackCollection)
                : _autoColl(autoColl), _rollbackCollection(rollbackCollection) {}
            void commit(boost::optional<Timestamp> commitTime) final {
                _autoColl._writableColl = nullptr;
            }
            void rollback() final {
                _autoColl._coll = _rollbackCollection;
                _autoColl._writableColl = nullptr;
            }

        private:
            AutoGetCollection& _autoColl;
            const Collection* _rollbackCollection;
        };

        _writableColl = CollectionCatalog::get(_opCtx).lookupCollectionByNamespaceForMetadataWrite(
            _opCtx, mode, _resolvedNss);
        if (mode == CollectionCatalog::LifetimeMode::kManagedInWriteUnitOfWork) {
            _opCtx->recoveryUnit()->registerChange(
                std::make_unique<WritableCollectionReset>(*this, _coll));
        }

        _coll = _writableColl;
    }
    return _writableColl;
}

CollectionWriter::CollectionWriter(OperationContext* opCtx,
                                   const CollectionUUID& uuid,
                                   CollectionCatalog::LifetimeMode mode)
    : _opCtx(opCtx), _mode(mode), _sharedThis(std::make_shared<CollectionWriter*>(this)) {

    _collection = CollectionCatalog::get(opCtx).lookupCollectionByUUID(opCtx, uuid);
    _lazyWritableCollectionInitializer = [opCtx, uuid](CollectionCatalog::LifetimeMode mode) {
        return CollectionCatalog::get(opCtx).lookupCollectionByUUIDForMetadataWrite(
            opCtx, mode, uuid);
    };
}

CollectionWriter::CollectionWriter(OperationContext* opCtx,
                                   const NamespaceString& nss,
                                   CollectionCatalog::LifetimeMode mode)
    : _opCtx(opCtx), _mode(mode), _sharedThis(std::make_shared<CollectionWriter*>(this)) {
    _collection = CollectionCatalog::get(opCtx).lookupCollectionByNamespace(opCtx, nss);
    _lazyWritableCollectionInitializer = [opCtx, nss](CollectionCatalog::LifetimeMode mode) {
        return CollectionCatalog::get(opCtx).lookupCollectionByNamespaceForMetadataWrite(
            opCtx, mode, nss);
    };
}

CollectionWriter::CollectionWriter(AutoGetCollection& autoCollection,
                                   CollectionCatalog::LifetimeMode mode)
    : _opCtx(autoCollection.getOperationContext()),
      _mode(mode),
      _sharedThis(std::make_shared<CollectionWriter*>(this)) {
    _collection = autoCollection.getCollection();
    _lazyWritableCollectionInitializer = [&autoCollection](CollectionCatalog::LifetimeMode mode) {
        return autoCollection.getWritableCollection(mode);
    };
}

CollectionWriter::CollectionWriter(Collection* writableCollection)
    : _collection(writableCollection),
      _writableCollection(writableCollection),
      _mode(CollectionCatalog::LifetimeMode::kInplace) {}

CollectionWriter::~CollectionWriter() {
    // Notify shared state that this instance is destroyed
    if (_sharedThis) {
        *_sharedThis = nullptr;
    }

    if (_mode == CollectionCatalog::LifetimeMode::kUnmanagedClone && _writableCollection) {
        CollectionCatalog::get(_opCtx).discardUnmanagedClone(_opCtx, _writableCollection);
    }
}

Collection* CollectionWriter::getWritableCollection() {
    // Acquire writable instance lazily if not already available
    if (!_writableCollection) {
        _writableCollection = _lazyWritableCollectionInitializer(_mode);

        // Resets the writable Collection when the write unit of work finishes so we re-fetch and
        // re-clone the Collection if a new write unit of work is opened. Holds the back pointer to
        // the CollectionWriter via a shared_ptr so we can detect if the instance is already
        // destroyed.
        class WritableCollectionReset : public RecoveryUnit::Change {
        public:
            WritableCollectionReset(std::shared_ptr<CollectionWriter*> sharedThis,
                                    const Collection* rollbackCollection)
                : _sharedThis(std::move(sharedThis)), _rollbackCollection(rollbackCollection) {}
            void commit(boost::optional<Timestamp> commitTime) final {
                if (*_sharedThis)
                    (*_sharedThis)->_writableCollection = nullptr;
            }
            void rollback() final {
                if (*_sharedThis) {
                    (*_sharedThis)->_collection = _rollbackCollection;
                    (*_sharedThis)->_writableCollection = nullptr;
                }
            }

        private:
            std::shared_ptr<CollectionWriter*> _sharedThis;
            const Collection* _rollbackCollection;
        };

        if (_mode == CollectionCatalog::LifetimeMode::kManagedInWriteUnitOfWork) {
            _opCtx->recoveryUnit()->registerChange(
                std::make_unique<WritableCollectionReset>(_sharedThis, _collection));
        }

        _collection = _writableCollection;
    }
    return _writableCollection;
}

void CollectionWriter::commitToCatalog() {
    dassert(_mode == CollectionCatalog::LifetimeMode::kUnmanagedClone);
    dassert(_writableCollection);
    CollectionCatalog::get(_opCtx).commitUnmanagedClone(_opCtx, _writableCollection);
    _writableCollection = nullptr;
}

CatalogCollectionLookup::CollectionStorage CatalogCollectionLookup::lookupCollection(
    OperationContext* opCtx, const NamespaceString& nss) {
    return CollectionCatalog::get(opCtx).lookupCollectionByNamespace(opCtx, nss);
}

CatalogCollectionLookupForRead::CollectionStorage CatalogCollectionLookupForRead::lookupCollection(
    OperationContext* opCtx, const NamespaceString& nss) {
    return CollectionCatalog::get(opCtx).lookupCollectionByNamespaceForRead(opCtx, nss);
}

LockMode fixLockModeForSystemDotViewsChanges(const NamespaceString& nss, LockMode mode) {
    return nss.isSystemDotViews() ? MODE_X : mode;
}

AutoGetOrCreateDb::AutoGetOrCreateDb(OperationContext* opCtx,
                                     StringData dbName,
                                     LockMode mode,
                                     Date_t deadline)
    : _autoDb(opCtx, dbName, mode, deadline), _db(_autoDb.ensureDbExists()) {
    invariant(mode == MODE_IX || mode == MODE_X);
}

ConcealCollectionCatalogChangesBlock::ConcealCollectionCatalogChangesBlock(OperationContext* opCtx)
    : _opCtx(opCtx) {
    CollectionCatalog::get(_opCtx).onCloseCatalog(_opCtx);
}

ConcealCollectionCatalogChangesBlock::~ConcealCollectionCatalogChangesBlock() {
    invariant(_opCtx);
    CollectionCatalog::get(_opCtx).onOpenCatalog(_opCtx);
}

ReadSourceScope::ReadSourceScope(OperationContext* opCtx,
                                 RecoveryUnit::ReadSource readSource,
                                 boost::optional<Timestamp> provided)
    : _opCtx(opCtx), _originalReadSource(opCtx->recoveryUnit()->getTimestampReadSource()) {

    if (_originalReadSource == RecoveryUnit::ReadSource::kProvided) {
        _originalReadTimestamp = *_opCtx->recoveryUnit()->getPointInTimeReadTimestamp();
    }

    _opCtx->recoveryUnit()->abandonSnapshot();
    _opCtx->recoveryUnit()->setTimestampReadSource(readSource, provided);
}

ReadSourceScope::~ReadSourceScope() {
    _opCtx->recoveryUnit()->abandonSnapshot();
    if (_originalReadSource == RecoveryUnit::ReadSource::kProvided) {
        _opCtx->recoveryUnit()->setTimestampReadSource(_originalReadSource, _originalReadTimestamp);
    } else {
        _opCtx->recoveryUnit()->setTimestampReadSource(_originalReadSource);
    }
}

AutoGetOplog::AutoGetOplog(OperationContext* opCtx, OplogAccessMode mode, Date_t deadline)
    : _shouldNotConflictWithSecondaryBatchApplicationBlock(opCtx->lockState()) {
    auto lockMode = (mode == OplogAccessMode::kRead) ? MODE_IS : MODE_IX;
    if (mode == OplogAccessMode::kLogOp) {
        // Invariant that global lock is already held for kLogOp mode.
        invariant(opCtx->lockState()->isWriteLocked());
    } else {
        _globalLock.emplace(opCtx, lockMode, deadline, Lock::InterruptBehavior::kThrow);
    }

    _oplogInfo = repl::LocalOplogInfo::get(opCtx);
    _oplog = _oplogInfo->getCollection();
}

template class AutoGetCollectionBase<CatalogCollectionLookup>;
template class AutoGetCollectionBase<CatalogCollectionLookupForRead>;

}  // namespace mongo
