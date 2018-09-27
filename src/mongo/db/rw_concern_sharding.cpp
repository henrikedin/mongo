/**
*    Copyright (C) 2016 MongoDB Inc.
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
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/db/logical_clock.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/read_concern.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/s/grid.h"
#include "mongo/util/concurrency/notification.h"
#include "mongo/util/log.h"

namespace mongo {

namespace {

/**
*  Synchronize writeRequests
*/

class WriteRequestSynchronizer;
const auto getWriteRequestsSynchronizer =
    ServiceContext::declareDecoration<WriteRequestSynchronizer>();

class WriteRequestSynchronizer {
public:
    WriteRequestSynchronizer() = default;

    /**
    * Returns a tuple <false, existingWriteRequest> if it can  find the one that happened after or
    * at clusterTime.
    * Returns a tuple <true, newWriteRequest> otherwise.
    */
    std::tuple<bool, std::shared_ptr<Notification<Status>>> getOrCreateWriteRequest(
        LogicalTime clusterTime) {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        auto lastEl = _writeRequests.rbegin();
        if (lastEl != _writeRequests.rend() && lastEl->first >= clusterTime.asTimestamp()) {
            return std::make_tuple(false, lastEl->second);
        } else {
            auto newWriteRequest = std::make_shared<Notification<Status>>();
            _writeRequests[clusterTime.asTimestamp()] = newWriteRequest;
            return std::make_tuple(true, newWriteRequest);
        }
    }

    /**
    * Erases writeRequest that happened at clusterTime
    */
    void deleteWriteRequest(LogicalTime clusterTime) {
        stdx::unique_lock<stdx::mutex> lock(_mutex);
        auto el = _writeRequests.find(clusterTime.asTimestamp());
        invariant(el != _writeRequests.end());
        invariant(el->second);
        el->second.reset();
        _writeRequests.erase(el);
    }

private:
    stdx::mutex _mutex;
    std::map<Timestamp, std::shared_ptr<Notification<Status>>> _writeRequests;
};
}  // namespace

MONGO_REGISTER_SHIM(attemptAppendOpLogNoteSharding)
(OperationContext* opCtx, LogicalTime clusterTime, LogicalTime lastAppliedOpTime)->Status {
    auto status = Status::OK();
    int remainingAttempts = 3;

    auto& writeRequests = getWriteRequestsSynchronizer(opCtx->getClient()->getServiceContext());
    repl::ReplicationCoordinator* const replCoord = repl::ReplicationCoordinator::get(opCtx);

    // this loop addresses the case when two or more threads need to advance the opLog time but the
    // one that waits for the notification gets the later clusterTime, so when the request finishes
    // it needs to be repeated with the later time.
    while (clusterTime > lastAppliedOpTime) {
        auto shardingState = ShardingState::get(opCtx);
        // standalone replica set, so there is no need to advance the OpLog on the primary.
        if (!shardingState->enabled()) {
            return Status::OK();
        }

        auto myShard = Grid::get(opCtx)->shardRegistry()->getShard(opCtx, shardingState->shardId());
        if (!myShard.isOK()) {
            return myShard.getStatus();
        }

        if (!remainingAttempts--) {
            std::stringstream ss;
            ss << "Requested clusterTime " << clusterTime.toString()
               << " is greater than the last primary OpTime: " << lastAppliedOpTime.toString()
               << " no retries left";
            return Status(ErrorCodes::InternalError, ss.str());
        }

        auto myWriteRequest = writeRequests.getOrCreateWriteRequest(clusterTime);
        if (std::get<0>(myWriteRequest)) {  // Its a new request
            try {
                LOG(2) << "New appendOplogNote request on clusterTime: " << clusterTime.toString()
                       << " remaining attempts: " << remainingAttempts;
                auto swRes = myShard.getValue()->runCommand(
                    opCtx,
                    ReadPreferenceSetting(ReadPreference::PrimaryOnly),
                    "admin",
                    BSON("appendOplogNote" << 1 << "maxClusterTime" << clusterTime.asTimestamp()
                                           << "data"
                                           << BSON("noop write for afterClusterTime read concern"
                                                   << 1)),
                    Shard::RetryPolicy::kIdempotent);
                status = swRes.getStatus();
                std::get<1>(myWriteRequest)->set(status);
                writeRequests.deleteWriteRequest(clusterTime);
            } catch (const DBException& ex) {
                status = ex.toStatus();
                // signal the writeRequest to unblock waiters
                std::get<1>(myWriteRequest)->set(status);
                writeRequests.deleteWriteRequest(clusterTime);
            }
        } else {
            LOG(2) << "Join appendOplogNote request on clusterTime: " << clusterTime.toString()
                   << " remaining attempts: " << remainingAttempts;
            try {
                status = std::get<1>(myWriteRequest)->get(opCtx);
            } catch (const DBException& ex) {
                return ex.toStatus();
            }
        }
        // If the write status is ok need to wait for the oplog to replicate.
        if (status.isOK()) {
            return status;
        }
        lastAppliedOpTime = LogicalTime(replCoord->getMyLastAppliedOpTime().getTimestamp());
    }
    // This is when the noop write failed but the opLog caught up to clusterTime by replicating.
    if (!status.isOK()) {
        LOG(1) << "Reached clusterTime " << lastAppliedOpTime.toString()
               << " but failed noop write due to " << status.toString();
    }

    return Status::OK();
}

}  // namespace mongo