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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/db/s/metadata_manager.h"

#include <memory>

#include "mongo/base/string_data.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/bson/util/builder.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/range_arithmetic.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/logv2/log.h"
#include "mongo/s/grid.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

// MetadataManager maintains pointers to CollectionMetadata objects in a member list named
// _metadata.  Each CollectionMetadata contains an immutable _chunksMap of chunks assigned to this
// shard, along with details related to its own lifecycle in a member _tracker.
//
// The current chunk mapping, used by queries starting up, is at _metadata.back().  Each query,
// when it starts up, requests and holds a ScopedCollectionMetadata object, and destroys it on
// termination. Each ScopedCollectionMetadata keeps a shared_ptr to its CollectionMetadata chunk
// mapping, and to the MetadataManager itself.  CollectionMetadata mappings also keep a record of
// chunk ranges that may be deleted when it is determined that the range can no longer be in use.
//
// ScopedCollectionMetadata's destructor decrements the CollectionMetadata's usageCounter.
// Whenever a usageCounter drops to zero, we check whether any now-unused CollectionMetadata
// elements can be popped off the front of _metadata.  We need to keep the unused elements in the
// middle (as seen below) because they may schedule deletions of chunks depended on by older
// mappings.
//
// New chunk mappings are pushed onto the back of _metadata. Subsequently started queries use the
// new mapping while still-running queries continue using the older "snapshot" mappings.  We treat
// _metadata.back()'s usage count differently from the snapshots because it can't reliably be
// compared to zero; a new query may increment it at any time.
//
// (Note that the collection may be dropped or become unsharded, and even get made and sharded
// again, between construction and destruction of a ScopedCollectionMetadata).
//
// MetadataManager also contains a CollectionRangeDeleter _rangesToClean that queues orphan ranges
// being deleted in a background thread, and a mapping _receivingChunks of the ranges being migrated
// in, to avoid deleting them.  Each range deletion is paired with a notification object triggered
// when the deletion is completed or abandoned.
//
//                                        ____________________________
//  (s): std::shared_ptr<>       Clients:| ScopedCollectionMetadata   |
//   _________________________        +----(s) manager   metadata (s)------------------+
//  | CollectionShardingState |       |  |____________________________|  |             |
//  |  _metadataManager (s)   |       +-------(s) manager  metadata (s)--------------+ |
//  |____________________|____|       |     |____________________________|   |       | |
//   ____________________v________    +------------(s) manager  metadata (s)-----+   | |
//  | MetadataManager             |   |         |____________________________|   |   | |
//  |                             |<--+                                          |   | |
//  |                             |        ___________________________  (1 use)  |   | |
//  | getActiveMetadata():    /---------->| CollectionMetadata        |<---------+   | |
//  |     back(): [(s),------/    |       |  _________________________|_             | |
//  |              (s),-------------------->| CollectionMetadata        | (0 uses)   | |
//  |  _metadata:  (s)]------\    |       | |  _________________________|_           | |
//  |                         \-------------->| CollectionMetadata        |          | |
//  |  _receivingChunks           |       | | |                           | (2 uses) | |
//  |  _rangesToClean:            |       | | |  _tracker:                |<---------+ |
//  |  _________________________  |       | | |  _______________________  |<-----------+
//  | | CollectionRangeDeleter  | |       | | | | Tracker               | |
//  | |                         | |       | | | |                       | |
//  | |  _orphans [range,notif, | |       | | | | usageCounter          | |
//  | |            range,notif, | |       | | | | orphans [range,notif, | |
//  | |                 ...   ] | |       | | | |          range,notif, | |
//  | |                         | |       | | | |              ...    ] | |
//  | |_________________________| |       |_| | |_______________________| |
//  |_____________________________|         | |  _chunksMap               |
//                                          |_|  _chunkVersion            |
//                                            |  ...                      |
//                                            |___________________________|
//
//  Note that _metadata as shown here has its front() at the bottom, back() at the top. As usual,
//  new entries are pushed onto the back, popped off the front.

namespace mongo {
namespace {

using TaskExecutor = executor::TaskExecutor;
using CallbackArgs = TaskExecutor::CallbackArgs;

MONGO_FAIL_POINT_DEFINE(suspendRangeDeletion);

/**
 * Deletes ranges, in background, until done, normally using a task executor attached to the
 * ShardingState.
 *
 * Each time it completes cleaning up a range, it wakes up clients waiting on completion of that
 * range, which may then verify that their range has no more deletions scheduled, and proceed.
 */
void scheduleCleanup(executor::TaskExecutor* executor,
                     NamespaceString nss,
                     UUID collectionUuid,
                     Date_t when) {
    LOGV2_DEBUG(1, "Scheduling cleanup on {} at {}", "nss_ns"_attr = nss.ns(), "when"_attr = when);
    auto swCallbackHandle = executor->scheduleWorkAt(
        when, [executor, nss = std::move(nss), uuid = collectionUuid](auto& args) {
            auto& status = args.status;
            if (ErrorCodes::isCancelationError(status.code())) {
                return;
            }
            invariant(status);

            ThreadClient tc("Collection-Range-Deleter", getGlobalServiceContext());
            {
                stdx::lock_guard<Client> lk(*tc.get());
                tc->setSystemOperationKillable(lk);
            }
            auto uniqueOpCtx = Client::getCurrent()->makeOperationContext();
            auto opCtx = uniqueOpCtx.get();

            suspendRangeDeletion.pauseWhileSet();

            auto next = CollectionRangeDeleter::cleanUpNextRange(opCtx, nss, uuid);
            if (next) {
                scheduleCleanup(executor, std::move(nss), std::move(uuid), *next);
            }
        });

    if (!swCallbackHandle.isOK()) {
        LOGV2("Failed to schedule the orphan data cleanup task{}", "causedBy_redact_swCallbackHandle_getStatus"_attr = causedBy(redact(swCallbackHandle.getStatus())));
    }
}

}  // namespace

class RangePreserver : public ScopedCollectionMetadata::Impl {
public:
    // Must be called locked with the MetadataManager's _managerLock
    RangePreserver(WithLock,
                   std::shared_ptr<MetadataManager> metadataManager,
                   std::shared_ptr<MetadataManager::CollectionMetadataTracker> metadataTracker)
        : _metadataManager(std::move(metadataManager)),
          _metadataTracker(std::move(metadataTracker)) {
        ++_metadataTracker->usageCounter;
    }

    ~RangePreserver() {
        stdx::lock_guard<Latch> managerLock(_metadataManager->_managerLock);

        invariant(_metadataTracker->usageCounter != 0);
        if (--_metadataTracker->usageCounter == 0) {
            // MetadataManager doesn't care which usageCounter went to zero. It just retires all
            // that are older than the oldest metadata still in use by queries (some start out at
            // zero, some go to zero but can't be expired yet).
            //
            // Note that new instances of ScopedCollectionMetadata may get attached to
            // _metadata.back(), so its usage count can increase from zero, unlike other reference
            // counts.
            _metadataManager->_retireExpiredMetadata(managerLock);
        }
    }

    // This will only ever refer to the active metadata, so CollectionMetadata should never be
    // boost::none
    const CollectionMetadata& get() {
        invariant(_metadataTracker->metadata);
        return _metadataTracker->metadata.get();
    }

private:
    friend boost::optional<ScopedCollectionMetadata> MetadataManager::getActiveMetadata(
        std::shared_ptr<MetadataManager>, const boost::optional<LogicalTime>&);

    std::shared_ptr<MetadataManager> _metadataManager;
    std::shared_ptr<MetadataManager::CollectionMetadataTracker> _metadataTracker;
};

MetadataManager::MetadataManager(ServiceContext* serviceContext,
                                 NamespaceString nss,
                                 TaskExecutor* executor)
    : _serviceContext(serviceContext),
      _nss(std::move(nss)),
      _executor(executor),
      _receivingChunks(SimpleBSONObjComparator::kInstance.makeBSONObjIndexedMap<BSONObj>()) {}

MetadataManager::~MetadataManager() {
    clearFilteringMetadata();
}

void MetadataManager::_clearAllCleanups(WithLock lock) {
    _clearAllCleanups(lock,
                      {ErrorCodes::InterruptedDueToReplStateChange,
                       str::stream()
                           << "Range deletions in " << _nss.ns()
                           << " abandoned because collection was dropped or became unsharded"});
}

void MetadataManager::_clearAllCleanups(WithLock, Status status) {
    for (auto& tracker : _metadata) {
        std::ignore = _rangesToClean.add(std::move(tracker->orphans));
    }
    _rangesToClean.clear(status);
}

boost::optional<ScopedCollectionMetadata> MetadataManager::getActiveMetadata(
    std::shared_ptr<MetadataManager> self, const boost::optional<LogicalTime>& atClusterTime) {
    stdx::lock_guard<Latch> lg(_managerLock);

    if (_metadata.empty()) {
        return boost::none;
    }

    auto activeMetadataTracker = _metadata.back();
    const auto& activeMetadata = activeMetadataTracker->metadata;

    // We don't keep routing history for unsharded collections, so if the collection is unsharded
    // just return the active metadata
    if (!atClusterTime || !activeMetadata->isSharded()) {
        return ScopedCollectionMetadata(std::make_shared<RangePreserver>(
            lg, std::move(self), std::move(activeMetadataTracker)));
    }

    auto chunkManager = activeMetadata->getChunkManager();
    auto chunkManagerAtClusterTime = std::make_shared<ChunkManager>(
        chunkManager->getRoutingHistory(), atClusterTime->asTimestamp());

    class MetadataAtTimestamp : public ScopedCollectionMetadata::Impl {
    public:
        MetadataAtTimestamp(CollectionMetadata metadata) : _metadata(std::move(metadata)) {}

        const CollectionMetadata& get() override {
            return _metadata;
        }

    private:
        CollectionMetadata _metadata;
    };

    return ScopedCollectionMetadata(std::make_shared<MetadataAtTimestamp>(
        CollectionMetadata(chunkManagerAtClusterTime, activeMetadata->shardId())));
}

size_t MetadataManager::numberOfMetadataSnapshots() const {
    stdx::lock_guard<Latch> lg(_managerLock);
    if (_metadata.empty())
        return 0;

    return _metadata.size() - 1;
}

int MetadataManager::numberOfEmptyMetadataSnapshots() const {
    stdx::lock_guard<Latch> lg(_managerLock);

    int emptyMetadataSnapshots = 0;
    for (const auto& collMetadataTracker : _metadata) {
        if (!collMetadataTracker->metadata)
            emptyMetadataSnapshots++;
    }

    return emptyMetadataSnapshots;
}

void MetadataManager::setFilteringMetadata(CollectionMetadata remoteMetadata) {
    stdx::lock_guard<Latch> lg(_managerLock);

    // Collection is becoming sharded
    if (_metadata.empty()) {
        LOGV2("Marking collection {} as {}", "_nss_ns"_attr = _nss.ns(), "remoteMetadata_toStringBasic"_attr = remoteMetadata.toStringBasic());

        invariant(_receivingChunks.empty());
        invariant(_rangesToClean.isEmpty());

        _setActiveMetadata(lg, std::move(remoteMetadata));
        return;
    }

    const auto& activeMetadata = _metadata.back()->metadata;

    // If the metadata being installed is unsharded or is sharded and has a different UUID from
    // ours, this means the collection was dropped and recreated, so we must entirely reset the
    // metadata state.
    if (!remoteMetadata.isSharded() ||
        (activeMetadata->isSharded() &&
         *activeMetadata->getChunkManager()->getUUID() !=
             remoteMetadata.getChunkManager()->getUUID())) {
        LOGV2("Updating metadata for collection {} from {} to {} due to UUID change", "_nss_ns"_attr = _nss.ns(), "activeMetadata_toStringBasic"_attr = activeMetadata->toStringBasic(), "remoteMetadata_toStringBasic"_attr = remoteMetadata.toStringBasic());

        _receivingChunks.clear();
        _clearAllCleanups(lg);
        _metadata.clear();

        _setActiveMetadata(lg, std::move(remoteMetadata));
        return;
    }

    // We already have the same or newer version
    if (activeMetadata->getCollVersion().epoch() == remoteMetadata.getCollVersion().epoch() &&
        activeMetadata->getCollVersion() >= remoteMetadata.getCollVersion()) {
        LOGV2_DEBUG(1, "Ignoring update of active metadata {} with an older {}", "activeMetadata_toStringBasic"_attr = activeMetadata->toStringBasic(), "remoteMetadata_toStringBasic"_attr = remoteMetadata.toStringBasic());
        return;
    }

    LOGV2("Updating metadata for collection {} from {} to {} due to version change", "_nss_ns"_attr = _nss.ns(), "activeMetadata_toStringBasic"_attr = activeMetadata->toStringBasic(), "remoteMetadata_toStringBasic"_attr = remoteMetadata.toStringBasic());

    // Resolve any receiving chunks, which might have completed by now
    for (auto it = _receivingChunks.begin(); it != _receivingChunks.end();) {
        const ChunkRange receivingRange(it->first, it->second);

        if (!remoteMetadata.rangeOverlapsChunk(receivingRange)) {
            ++it;
            continue;
        }

        // The remote metadata contains a chunk we were earlier in the process of receiving, so we
        // deem it successfully received
        LOGV2_DEBUG(2, "Verified chunk {} for collection {} has been migrated to this shard earlier", "redact_receivingRange_toString"_attr = redact(receivingRange.toString()), "_nss_ns"_attr = _nss.ns());

        _receivingChunks.erase(it);
        it = _receivingChunks.begin();
    }

    _setActiveMetadata(lg, std::move(remoteMetadata));
}

void MetadataManager::clearFilteringMetadata() {
    stdx::lock_guard<Latch> lg(_managerLock);
    _receivingChunks.clear();
    _clearAllCleanups(lg);
    _metadata.clear();
}

void MetadataManager::_setActiveMetadata(WithLock wl, CollectionMetadata newMetadata) {
    _metadata.emplace_back(std::make_shared<CollectionMetadataTracker>(std::move(newMetadata)));
    _retireExpiredMetadata(wl);
}

void MetadataManager::_retireExpiredMetadata(WithLock lock) {
    // Remove entries and schedule orphans for deletion only from the front of _metadata. We cannot
    // remove an entry from the middle of _metadata because a previous entry (whose usageCount is
    // not 0) could have a query that is actually still accessing those documents.
    while (_metadata.size() > 1 && !_metadata.front()->usageCounter) {
        if (!_metadata.front()->orphans.empty()) {
            LOG(0) << "Queries possibly dependent on " << _nss.ns()
                   << " range(s) finished; scheduling ranges for deletion";

            _pushListToClean(lock, std::move(_metadata.front()->orphans));
        }

        _metadata.pop_front();
    }

    // To avoid memory build up of ChunkManager objects, we can clear the CollectionMetadata object
    // in an entry when its usageCount is 0 as long as it is not the last item in _metadata (which
    // is the active metadata). If _metadata is empty, decrementing iter will be out of bounds, so
    // we must check that the size is > 1 as well.
    if (_metadata.size() > 1) {
        auto iter = _metadata.begin();
        while (iter != (--_metadata.end())) {
            if ((*iter)->usageCounter == 0) {
                (*iter)->metadata = boost::none;
            }
            ++iter;
        }
    }
}

void MetadataManager::toBSONPending(BSONArrayBuilder& bb) const {
    stdx::lock_guard<Latch> lg(_managerLock);

    for (auto it = _receivingChunks.begin(); it != _receivingChunks.end(); ++it) {
        BSONArrayBuilder pendingBB(bb.subarrayStart());
        pendingBB.append(it->first);
        pendingBB.append(it->second);
        pendingBB.done();
    }
}

void MetadataManager::append(BSONObjBuilder* builder) const {
    stdx::lock_guard<Latch> lg(_managerLock);

    _rangesToClean.append(builder);

    BSONArrayBuilder pcArr(builder->subarrayStart("pendingChunks"));
    for (const auto& entry : _receivingChunks) {
        BSONObjBuilder obj;
        ChunkRange r = ChunkRange(entry.first, entry.second);
        r.append(&obj);
        pcArr.append(obj.done());
    }
    pcArr.done();

    if (_metadata.empty()) {
        return;
    }

    BSONArrayBuilder amrArr(builder->subarrayStart("activeMetadataRanges"));
    for (const auto& entry : _metadata.back()->metadata->getChunks()) {
        BSONObjBuilder obj;
        ChunkRange r = ChunkRange(entry.first, entry.second);
        r.append(&obj);
        amrArr.append(obj.done());
    }
    amrArr.done();
}

auto MetadataManager::_pushRangeToClean(WithLock lock, ChunkRange const& range, Date_t when)
    -> CleanupNotification {
    std::list<Deletion> ranges;
    ranges.emplace_back(ChunkRange(range.getMin().getOwned(), range.getMax().getOwned()), when);
    auto& notifn = ranges.back().notification;
    _pushListToClean(lock, std::move(ranges));
    return notifn;
}

void MetadataManager::_pushListToClean(WithLock, std::list<Deletion> ranges) {
    auto when = _rangesToClean.add(std::move(ranges));
    if (when) {
        auto collectionUuid = _metadata.back()->metadata->getChunkManager()->getUUID();
        invariant(collectionUuid);
        scheduleCleanup(_executor, _nss, *collectionUuid, *when);
    }
}

auto MetadataManager::beginReceive(ChunkRange const& range) -> CleanupNotification {
    stdx::lock_guard<Latch> lg(_managerLock);
    invariant(!_metadata.empty());

    if (_overlapsInUseChunk(lg, range)) {
        return Status{ErrorCodes::RangeOverlapConflict,
                      "Documents in target range may still be in use on the destination shard."};
    }

    _receivingChunks.emplace(range.getMin().getOwned(), range.getMax().getOwned());

    LOGV2("Scheduling deletion of any documents in {} range {} before migrating in a chunk covering the range", "_nss_ns"_attr = _nss.ns(), "redact_range_toString"_attr = redact(range.toString()));

    return _pushRangeToClean(lg, range, Date_t{});
}

void MetadataManager::forgetReceive(ChunkRange const& range) {
    stdx::lock_guard<Latch> lg(_managerLock);
    invariant(!_metadata.empty());

    // This is potentially a partially received chunk, which needs to be cleaned up. We know none
    // of these documents are in use, so they can go straight to the deletion queue.
    log() << "Abandoning in-migration of " << _nss.ns() << " range " << range
          << "; scheduling deletion of any documents already copied";

    invariant(!_overlapsInUseChunk(lg, range));

    auto it = _receivingChunks.find(range.getMin());
    invariant(it != _receivingChunks.end());
    _receivingChunks.erase(it);

    _pushRangeToClean(lg, range, Date_t{}).abandon();
}

auto MetadataManager::cleanUpRange(ChunkRange const& range, Date_t whenToDelete)
    -> CleanupNotification {
    stdx::lock_guard<Latch> lg(_managerLock);
    invariant(!_metadata.empty());

    auto* const activeMetadata = _metadata.back().get();
    auto* const overlapMetadata = _findNewestOverlappingMetadata(lg, range);

    if (overlapMetadata == activeMetadata) {
        return Status{ErrorCodes::RangeOverlapConflict,
                      str::stream() << "Requested deletion range overlaps a live shard chunk"};
    }

    if (rangeMapOverlaps(_receivingChunks, range.getMin(), range.getMax())) {
        return Status{ErrorCodes::RangeOverlapConflict,
                      str::stream() << "Requested deletion range overlaps a chunk being"
                                       " migrated in"};
    }

    if (!overlapMetadata) {
        // No running queries can depend on it, so queue it for deletion immediately.
        const auto whenStr = (whenToDelete == Date_t{}) ? "immediate"_sd : "deferred"_sd;
        LOGV2("Scheduling {} deletion of {} range {}", "whenStr"_attr = whenStr, "_nss_ns"_attr = _nss.ns(), "redact_range_toString"_attr = redact(range.toString()));
        return _pushRangeToClean(lg, range, whenToDelete);
    }

    LOGV2("Deletion of {} range {} will be scheduled after all possibly dependent queries finish", "_nss_ns"_attr = _nss.ns(), "redact_range_toString"_attr = redact(range.toString()));

    // Put it on the oldest metadata permissible; the current one might live a long time.
    auto& orphans = overlapMetadata->orphans;
    orphans.emplace_back(ChunkRange(range.getMin().getOwned(), range.getMax().getOwned()),
                         whenToDelete);

    return orphans.back().notification;
}

size_t MetadataManager::numberOfRangesToCleanStillInUse() const {
    stdx::lock_guard<Latch> lg(_managerLock);
    size_t count = 0;
    for (auto& tracker : _metadata) {
        count += tracker->orphans.size();
    }
    return count;
}

size_t MetadataManager::numberOfRangesToClean() const {
    stdx::lock_guard<Latch> lg(_managerLock);
    return _rangesToClean.size();
}

auto MetadataManager::trackOrphanedDataCleanup(ChunkRange const& range) const
    -> boost::optional<CleanupNotification> {
    stdx::lock_guard<Latch> lg(_managerLock);
    auto overlaps = _overlapsInUseCleanups(lg, range);
    if (overlaps) {
        return overlaps;
    }

    return _rangesToClean.overlaps(range);
}

auto MetadataManager::_findNewestOverlappingMetadata(WithLock, ChunkRange const& range)
    -> CollectionMetadataTracker* {
    invariant(!_metadata.empty());

    auto it = _metadata.rbegin();
    if ((*it)->metadata && (*it)->metadata->rangeOverlapsChunk(range)) {
        return (*it).get();
    }

    ++it;
    for (; it != _metadata.rend(); ++it) {
        auto& tracker = *it;
        if (tracker->usageCounter && tracker->metadata &&
            tracker->metadata->rangeOverlapsChunk(range)) {
            return tracker.get();
        }
    }

    return nullptr;
}

bool MetadataManager::_overlapsInUseChunk(WithLock lk, ChunkRange const& range) {
    auto* cm = _findNewestOverlappingMetadata(lk, range);
    return (cm != nullptr);
}

auto MetadataManager::_overlapsInUseCleanups(WithLock, ChunkRange const& range) const
    -> boost::optional<CleanupNotification> {
    invariant(!_metadata.empty());

    for (auto it = _metadata.rbegin(); it != _metadata.rend(); ++it) {
        const auto& orphans = (*it)->orphans;
        for (auto itOrphans = orphans.rbegin(); itOrphans != orphans.rend(); ++itOrphans) {
            const auto& orphan = *itOrphans;
            if (orphan.range.overlapWith(range)) {
                return orphan.notification;
            }
        }
    }

    return boost::none;
}

boost::optional<ChunkRange> MetadataManager::getNextOrphanRange(BSONObj const& from) const {
    stdx::lock_guard<Latch> lg(_managerLock);
    invariant(!_metadata.empty());
    return _metadata.back()->metadata->getNextOrphanRange(_receivingChunks, from);
}

}  // namespace mongo
