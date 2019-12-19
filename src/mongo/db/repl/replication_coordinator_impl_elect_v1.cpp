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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplicationElection

#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/db/repl/replication_coordinator_impl.h"
#include "mongo/db/repl/replication_metrics.h"
#include "mongo/db/repl/topology_coordinator.h"
#include "mongo/db/repl/vote_requester.h"
#include "mongo/logv2/log.h"
#include "mongo/platform/mutex.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {
namespace repl {

class ReplicationCoordinatorImpl::LoseElectionGuardV1 {
    LoseElectionGuardV1(const LoseElectionGuardV1&) = delete;
    LoseElectionGuardV1& operator=(const LoseElectionGuardV1&) = delete;

public:
    LoseElectionGuardV1(ReplicationCoordinatorImpl* replCoord) : _replCoord(replCoord) {}

    virtual ~LoseElectionGuardV1() {
        if (_dismissed) {
            return;
        }
        LOGV2("Lost {}election due to internal error", "_isDryRun_dry_run"_attr = (_isDryRun ? "dry run " : ""));
        _replCoord->_topCoord->processLoseElection();
        _replCoord->_voteRequester.reset(nullptr);
        if (_isDryRun && _replCoord->_electionDryRunFinishedEvent.isValid()) {
            _replCoord->_replExecutor->signalEvent(_replCoord->_electionDryRunFinishedEvent);
        }
        if (_replCoord->_electionFinishedEvent.isValid()) {
            _replCoord->_replExecutor->signalEvent(_replCoord->_electionFinishedEvent);
        }

        // Clear the node's election candidate metrics if it loses either the dry-run or actual
        // election, since it will not become primary.
        ReplicationMetrics::get(getGlobalServiceContext()).clearElectionCandidateMetrics();
    }

    void dismiss() {
        _dismissed = true;
    }

protected:
    ReplicationCoordinatorImpl* const _replCoord;
    bool _isDryRun = false;
    bool _dismissed = false;
};

class ReplicationCoordinatorImpl::LoseElectionDryRunGuardV1 : public LoseElectionGuardV1 {
    LoseElectionDryRunGuardV1(const LoseElectionDryRunGuardV1&) = delete;
    LoseElectionDryRunGuardV1& operator=(const LoseElectionDryRunGuardV1&) = delete;

public:
    LoseElectionDryRunGuardV1(ReplicationCoordinatorImpl* replCoord)
        : LoseElectionGuardV1(replCoord) {
        _isDryRun = true;
    }
};

void ReplicationCoordinatorImpl::_startElectSelfV1(StartElectionReasonEnum reason) {
    stdx::lock_guard<Latch> lk(_mutex);
    _startElectSelfV1_inlock(reason);
}

void ReplicationCoordinatorImpl::_startElectSelfV1_inlock(StartElectionReasonEnum reason) {
    invariant(!_voteRequester);

    switch (_rsConfigState) {
        case kConfigSteady:
            break;
        case kConfigInitiating:
        case kConfigReconfiguring:
        case kConfigHBReconfiguring:
            LOG(2) << "Not standing for election; processing a configuration change";
            // Transition out of candidate role.
            _topCoord->processLoseElection();
            return;
        default:
            severe() << "Entered replica set election code while in illegal config state "
                     << int(_rsConfigState);
            fassertFailed(28641);
    }

    auto finishedEvent = _makeEvent();
    if (!finishedEvent) {
        return;
    }
    _electionFinishedEvent = finishedEvent;

    auto dryRunFinishedEvent = _makeEvent();
    if (!dryRunFinishedEvent) {
        return;
    }
    _electionDryRunFinishedEvent = dryRunFinishedEvent;

    LoseElectionDryRunGuardV1 lossGuard(this);


    invariant(_rsConfig.getMemberAt(_selfIndex).isElectable());
    const auto lastOpTime = _getMyLastAppliedOpTime_inlock();

    if (lastOpTime == OpTime()) {
        LOGV2("not trying to elect self, "
                 "do not yet have a complete set of data from any point in time");
        return;
    }

    long long term = _topCoord->getTerm();
    int primaryIndex = -1;

    if (reason == StartElectionReasonEnum::kStepUpRequestSkipDryRun) {
        long long newTerm = term + 1;
        LOGV2("skipping dry run and running for election in term {}", "newTerm"_attr = newTerm);
        _startRealElection_inlock(newTerm, reason);
        lossGuard.dismiss();
        return;
    }

    LOGV2("conducting a dry run election to see if we could be elected. current term: {}", "term"_attr = term);
    _voteRequester.reset(new VoteRequester);

    // Only set primaryIndex if the primary's vote is required during the dry run.
    if (reason == StartElectionReasonEnum::kCatchupTakeover) {
        primaryIndex = _topCoord->getCurrentPrimaryIndex();
    }
    StatusWith<executor::TaskExecutor::EventHandle> nextPhaseEvh =
        _voteRequester->start(_replExecutor.get(),
                              _rsConfig,
                              _selfIndex,
                              term,
                              true,  // dry run
                              lastOpTime,
                              primaryIndex);
    if (nextPhaseEvh.getStatus() == ErrorCodes::ShutdownInProgress) {
        return;
    }
    fassert(28685, nextPhaseEvh.getStatus());
    _replExecutor
        ->onEvent(nextPhaseEvh.getValue(),
                  [=](const executor::TaskExecutor::CallbackArgs&) {
                      _processDryRunResult(term, reason);
                  })
        .status_with_transitional_ignore();
    lossGuard.dismiss();
}

void ReplicationCoordinatorImpl::_processDryRunResult(long long originalTerm,
                                                      StartElectionReasonEnum reason) {
    stdx::lock_guard<Latch> lk(_mutex);
    LoseElectionDryRunGuardV1 lossGuard(this);

    invariant(_voteRequester);

    if (_topCoord->getTerm() != originalTerm) {
        LOGV2("not running for primary, we have been superseded already during dry run. original term: {}, current term: {}", "originalTerm"_attr = originalTerm, "_topCoord_getTerm"_attr = _topCoord->getTerm());
        return;
    }

    const VoteRequester::Result endResult = _voteRequester->getResult();

    if (endResult == VoteRequester::Result::kInsufficientVotes) {
        LOGV2("not running for primary, we received insufficient votes");
        return;
    } else if (endResult == VoteRequester::Result::kStaleTerm) {
        LOGV2("not running for primary, we have been superseded already");
        return;
    } else if (endResult == VoteRequester::Result::kPrimaryRespondedNo) {
        LOGV2("not running for primary, the current primary responded no in the dry run");
        return;
    } else if (endResult != VoteRequester::Result::kSuccessfullyElected) {
        LOGV2("not running for primary, we received an unexpected problem");
        return;
    }

    long long newTerm = originalTerm + 1;
    LOGV2("dry election run succeeded, running for election in term {}", "newTerm"_attr = newTerm);

    _startRealElection_inlock(newTerm, reason);
    lossGuard.dismiss();
}

void ReplicationCoordinatorImpl::_startRealElection_inlock(long long newTerm,
                                                           StartElectionReasonEnum reason) {

    const Date_t now = _replExecutor->now();
    const OpTime lastCommittedOpTime = _topCoord->getLastCommittedOpTime();
    const OpTime lastSeenOpTime = _topCoord->latestKnownOpTime();
    const int numVotesNeeded = _rsConfig.getMajorityVoteCount();
    const double priorityAtElection = _rsConfig.getMemberAt(_selfIndex).getPriority();
    const Milliseconds electionTimeoutMillis = _rsConfig.getElectionTimeoutPeriod();
    const int priorPrimaryIndex = _topCoord->getCurrentPrimaryIndex();
    const boost::optional<int> priorPrimaryMemberId = (priorPrimaryIndex == -1)
        ? boost::none
        : boost::make_optional(_rsConfig.getMemberAt(priorPrimaryIndex).getId().getData());

    ReplicationMetrics::get(getServiceContext())
        .setElectionCandidateMetrics(reason,
                                     now,
                                     newTerm,
                                     lastCommittedOpTime,
                                     lastSeenOpTime,
                                     numVotesNeeded,
                                     priorityAtElection,
                                     electionTimeoutMillis,
                                     priorPrimaryMemberId);
    ReplicationMetrics::get(getServiceContext()).incrementNumElectionsCalledForReason(reason);

    LoseElectionDryRunGuardV1 lossGuard(this);

    TopologyCoordinator::UpdateTermResult updateTermResult;
    _updateTerm_inlock(newTerm, &updateTermResult);
    // This is the only valid result from this term update. If we are here, then we are not a
    // primary, so a stepdown is not possible. We have also not yet learned of a higher term from
    // someone else: seeing an update in the topology coordinator mid-election requires releasing
    // the mutex. This only happens during a dry run, which makes sure to check for term updates.
    invariant(updateTermResult == TopologyCoordinator::UpdateTermResult::kUpdatedTerm);
    // Secure our vote for ourself first
    _topCoord->voteForMyselfV1();

    // Store the vote in persistent storage.
    LastVote lastVote{newTerm, _selfIndex};

    auto cbStatus = _replExecutor->scheduleWork(
        [this, lastVote, reason](const executor::TaskExecutor::CallbackArgs& cbData) {
            _writeLastVoteForMyElection(lastVote, cbData, reason);
        });
    if (cbStatus.getStatus() == ErrorCodes::ShutdownInProgress) {
        return;
    }
    fassert(34421, cbStatus.getStatus());
    lossGuard.dismiss();
}

void ReplicationCoordinatorImpl::_writeLastVoteForMyElection(
    LastVote lastVote,
    const executor::TaskExecutor::CallbackArgs& cbData,
    StartElectionReasonEnum reason) {
    // storeLocalLastVoteDocument can call back in to the replication coordinator,
    // so _mutex must be unlocked here.  However, we cannot return until we
    // lock it because we want to lose the election on cancel or error and
    // doing so requires _mutex.
    auto status = [&] {
        if (!cbData.status.isOK()) {
            return cbData.status;
        }
        auto opCtx = cc().makeOperationContext();
        // Any writes that occur as part of an election should not be subject to Flow Control.
        opCtx->setShouldParticipateInFlowControl(false);
        return _externalState->storeLocalLastVoteDocument(opCtx.get(), lastVote);
    }();

    stdx::lock_guard<Latch> lk(_mutex);
    LoseElectionDryRunGuardV1 lossGuard(this);
    if (status == ErrorCodes::CallbackCanceled) {
        return;
    }

    if (!status.isOK()) {
        LOGV2("failed to store LastVote document when voting for myself: {}", "status"_attr = status);
        return;
    }

    if (_topCoord->getTerm() != lastVote.getTerm()) {
        LOGV2("not running for primary, we have been superseded already while writing our last "
                 "vote. election term: {}, current term: {}", "lastVote_getTerm"_attr = lastVote.getTerm(), "_topCoord_getTerm"_attr = _topCoord->getTerm());
        return;
    }
    _startVoteRequester_inlock(lastVote.getTerm(), reason);
    _replExecutor->signalEvent(_electionDryRunFinishedEvent);

    lossGuard.dismiss();
}

void ReplicationCoordinatorImpl::_startVoteRequester_inlock(long long newTerm,
                                                            StartElectionReasonEnum reason) {
    const auto lastOpTime = _getMyLastAppliedOpTime_inlock();

    _voteRequester.reset(new VoteRequester);
    StatusWith<executor::TaskExecutor::EventHandle> nextPhaseEvh = _voteRequester->start(
        _replExecutor.get(), _rsConfig, _selfIndex, newTerm, false, lastOpTime, -1);
    if (nextPhaseEvh.getStatus() == ErrorCodes::ShutdownInProgress) {
        return;
    }
    fassert(28643, nextPhaseEvh.getStatus());
    _replExecutor
        ->onEvent(nextPhaseEvh.getValue(),
                  [=](const executor::TaskExecutor::CallbackArgs&) {
                      _onVoteRequestComplete(newTerm, reason);
                  })
        .status_with_transitional_ignore();
}

MONGO_FAIL_POINT_DEFINE(electionHangsBeforeUpdateMemberState);

void ReplicationCoordinatorImpl::_onVoteRequestComplete(long long newTerm,
                                                        StartElectionReasonEnum reason) {
    stdx::lock_guard<Latch> lk(_mutex);
    LoseElectionGuardV1 lossGuard(this);

    invariant(_voteRequester);

    if (_topCoord->getTerm() != newTerm) {
        LOGV2("not becoming primary, we have been superseded already during election. election term: {}, current term: {}", "newTerm"_attr = newTerm, "_topCoord_getTerm"_attr = _topCoord->getTerm());
        return;
    }

    const VoteRequester::Result endResult = _voteRequester->getResult();
    invariant(endResult != VoteRequester::Result::kPrimaryRespondedNo);

    switch (endResult) {
        case VoteRequester::Result::kInsufficientVotes:
            LOGV2("not becoming primary, we received insufficient votes");
            return;
        case VoteRequester::Result::kStaleTerm:
            LOGV2("not becoming primary, we have been superseded already");
            return;
        case VoteRequester::Result::kSuccessfullyElected:
            LOGV2("election succeeded, assuming primary role in term {}", "_topCoord_getTerm"_attr = _topCoord->getTerm());
            ReplicationMetrics::get(getServiceContext())
                .incrementNumElectionsSuccessfulForReason(reason);
            break;
        case VoteRequester::Result::kPrimaryRespondedNo:
            // This is impossible because we would only require the primary's
            // vote during a dry run.
            MONGO_UNREACHABLE;
    }

    // Mark all nodes that responded to our vote request as up to avoid immediately
    // relinquishing primary.
    Date_t now = _replExecutor->now();
    _topCoord->resetMemberTimeouts(now, _voteRequester->getResponders());

    _voteRequester.reset();
    auto electionFinishedEvent = _electionFinishedEvent;

    electionHangsBeforeUpdateMemberState.execute([&](const BSONObj& customWait) {
        auto waitForMillis = Milliseconds(customWait["waitForMillis"].numberInt());
        LOGV2("election succeeded - electionHangsBeforeUpdateMemberState fail point "
                 "enabled, sleeping {}", "waitForMillis"_attr = waitForMillis);
        sleepFor(waitForMillis);
    });

    _postWonElectionUpdateMemberState(lk);
    _replExecutor->signalEvent(electionFinishedEvent);
    lossGuard.dismiss();
}

}  // namespace repl
}  // namespace mongo
