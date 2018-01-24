/**
 *    Copyright (C) 2018 MongoDB Inc.
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

#pragma once

#include "mongo/db/repl/replication_coordinator.h"

namespace mongo {

class Timer;
template <typename T>
class StatusWith;


namespace repl {
class ReplSetConfig;

class ReplicationCoordinatorEmbedded : public ReplicationCoordinator {
    MONGO_DISALLOW_COPYING(ReplicationCoordinatorEmbedded);

public:
    ReplicationCoordinatorEmbedded(
        ServiceContext* serviceContext);

    virtual ~ReplicationCoordinatorEmbedded();

    // ================== Members of public ReplicationCoordinator API ===================

    virtual void startup(OperationContext* opCtx) override;

    virtual void shutdown(OperationContext* opCtx) override;

    virtual const ReplSettings& getSettings() const override;

    virtual Mode getReplicationMode() const override;

    virtual MemberState getMemberState() const override;

    virtual Status waitForMemberState(MemberState expectedState, Milliseconds timeout) override;

    virtual bool isInPrimaryOrSecondaryState() const override;

    virtual Seconds getSlaveDelaySecs() const override;

    virtual void clearSyncSourceBlacklist() override;

    virtual ReplicationCoordinator::StatusAndDuration awaitReplication(
        OperationContext* opCtx,
        const OpTime& opTime,
        const WriteConcernOptions& writeConcern) override;

    virtual Status stepDown(OperationContext* opCtx,
                            bool force,
                            const Milliseconds& waitTime,
                            const Milliseconds& stepdownTime) override;

    virtual bool isMasterForReportingPurposes() override;

    virtual bool canAcceptWritesForDatabase(OperationContext* opCtx, StringData dbName) override;
    virtual bool canAcceptWritesForDatabase_UNSAFE(OperationContext* opCtx,
                                                   StringData dbName) override;

    bool canAcceptWritesFor(OperationContext* opCtx, const NamespaceString& ns) override;
    bool canAcceptWritesFor_UNSAFE(OperationContext* opCtx, const NamespaceString& ns) override;

    virtual Status checkIfWriteConcernCanBeSatisfied(
        const WriteConcernOptions& writeConcern) const override;

    virtual Status checkCanServeReadsFor(OperationContext* opCtx,
                                         const NamespaceString& ns,
                                         bool slaveOk) override;
    virtual Status checkCanServeReadsFor_UNSAFE(OperationContext* opCtx,
                                                const NamespaceString& ns,
                                                bool slaveOk) override;

    virtual bool shouldRelaxIndexConstraints(OperationContext* opCtx,
                                             const NamespaceString& ns) override;

    virtual Status setLastOptimeForSlave(const OID& rid, const Timestamp& ts) override;

    virtual void setMyLastAppliedOpTime(const OpTime& opTime) override;
    virtual void setMyLastDurableOpTime(const OpTime& opTime) override;

    virtual void setMyLastAppliedOpTimeForward(const OpTime& opTime,
                                               DataConsistency consistency) override;
    virtual void setMyLastDurableOpTimeForward(const OpTime& opTime) override;

    virtual void resetMyLastOpTimes() override;

    virtual void setMyHeartbeatMessage(const std::string& msg) override;

    virtual OpTime getMyLastAppliedOpTime() const override;
    virtual OpTime getMyLastDurableOpTime() const override;

    virtual Status waitUntilOpTimeForReadUntil(OperationContext* opCtx,
                                               const ReadConcernArgs& readConcern,
                                               boost::optional<Date_t> deadline) override;

    virtual Status waitUntilOpTimeForRead(OperationContext* opCtx,
                                          const ReadConcernArgs& readConcern) override;

    virtual OID getElectionId() override;

    virtual OID getMyRID() const override;

    virtual int getMyId() const override;

    virtual Status setFollowerMode(const MemberState& newState) override;

    virtual ApplierState getApplierState() override;

    virtual void signalDrainComplete(OperationContext* opCtx,
                                     long long termWhenBufferIsEmpty) override;

    virtual Status waitForDrainFinish(Milliseconds timeout) override;

    virtual void signalUpstreamUpdater() override;

    virtual Status resyncData(OperationContext* opCtx, bool waitUntilCompleted) override;

    virtual StatusWith<BSONObj> prepareReplSetUpdatePositionCommand() const override;

    virtual Status processReplSetGetStatus(BSONObjBuilder* result,
                                           ReplSetGetStatusResponseStyle responseStyle) override;

    virtual void fillIsMasterForReplSet(IsMasterResponse* result) override;

    virtual void appendSlaveInfoData(BSONObjBuilder* result) override;

    virtual ReplSetConfig getConfig() const override;

    virtual void processReplSetGetConfig(BSONObjBuilder* result) override;

    virtual void processReplSetMetadata(const rpc::ReplSetMetadata& replMetadata) override;

    virtual void advanceCommitPoint(const OpTime& committedOpTime) override;

    virtual void cancelAndRescheduleElectionTimeout() override;

    virtual Status setMaintenanceMode(bool activate) override;

    virtual bool getMaintenanceMode() override;

    virtual Status processReplSetSyncFrom(OperationContext* opCtx,
                                          const HostAndPort& target,
                                          BSONObjBuilder* resultObj) override;

    virtual Status processReplSetFreeze(int secs, BSONObjBuilder* resultObj) override;

    virtual Status processHeartbeat(const ReplSetHeartbeatArgs& args,
                                    ReplSetHeartbeatResponse* response) override;

    virtual Status processReplSetReconfig(OperationContext* opCtx,
                                          const ReplSetReconfigArgs& args,
                                          BSONObjBuilder* resultObj) override;

    virtual Status processReplSetInitiate(OperationContext* opCtx,
                                          const BSONObj& configObj,
                                          BSONObjBuilder* resultObj) override;

    virtual Status processReplSetFresh(const ReplSetFreshArgs& args,
                                       BSONObjBuilder* resultObj) override;

    virtual Status processReplSetElect(const ReplSetElectArgs& args,
                                       BSONObjBuilder* response) override;

    virtual Status processReplSetUpdatePosition(const UpdatePositionArgs& updates,
                                                long long* configVersion) override;

    virtual Status processHandshake(OperationContext* opCtx,
                                    const HandshakeArgs& handshake) override;

    virtual bool buildsIndexes() override;

    virtual std::vector<HostAndPort> getHostsWrittenTo(const OpTime& op,
                                                       bool durablyWritten) override;

    virtual std::vector<HostAndPort> getOtherNodesInReplSet() const override;

    virtual WriteConcernOptions getGetLastErrorDefault() override;

    virtual Status checkReplEnabledForCommand(BSONObjBuilder* result) override;

    virtual bool isReplEnabled() const override;

    virtual HostAndPort chooseNewSyncSource(const OpTime& lastOpTimeFetched) override;

    virtual void blacklistSyncSource(const HostAndPort& host, Date_t until) override;

    virtual void resetLastOpTimesFromOplog(OperationContext* opCtx,
                                           DataConsistency consistency) override;

    virtual bool shouldChangeSyncSource(
        const HostAndPort& currentSource,
        const rpc::ReplSetMetadata& replMetadata,
        boost::optional<rpc::OplogQueryMetadata> oqMetadata) override;

    virtual OpTime getLastCommittedOpTime() const override;

    virtual Status processReplSetRequestVotes(OperationContext* opCtx,
                                              const ReplSetRequestVotesArgs& args,
                                              ReplSetRequestVotesResponse* response) override;

    virtual void prepareReplMetadata(OperationContext* opCtx,
                                     const BSONObj& metadataRequestObj,
                                     const OpTime& lastOpTimeFromClient,
                                     BSONObjBuilder* builder) const override;

    virtual Status processHeartbeatV1(const ReplSetHeartbeatArgsV1& args,
                                      ReplSetHeartbeatResponse* response) override;

    virtual bool isV1ElectionProtocol() const override;

    virtual bool getWriteConcernMajorityShouldJournal() override;

    virtual void summarizeAsHtml(ReplSetHtmlSummary* s) override;

    virtual void dropAllSnapshots() override;
    /**
     * Get current term from topology coordinator
     */
    virtual long long getTerm() override;

    // Returns the ServiceContext where this instance runs.
    virtual ServiceContext* getServiceContext() override {
        return _service;
    }

    virtual Status updateTerm(OperationContext* opCtx, long long term) override;

    virtual Timestamp getMinimumVisibleSnapshot(OperationContext* opCtx) override;

    virtual OpTime getCurrentCommittedSnapshotOpTime() const override;

    virtual void waitUntilSnapshotCommitted(OperationContext* opCtx,
                                            const Timestamp& untilSnapshot) override;

    virtual void appendDiagnosticBSON(BSONObjBuilder*) override;

    virtual void appendConnectionStats(executor::ConnectionPoolStats* stats) const override;

    virtual size_t getNumUncommittedSnapshots() override;

    virtual WriteConcernOptions populateUnsetWriteConcernOptionsSyncMode(
        WriteConcernOptions wc) override;

    virtual ReplSettings::IndexPrefetchConfig getIndexPrefetchConfig() const override;
    virtual void setIndexPrefetchConfig(const ReplSettings::IndexPrefetchConfig cfg) override;

    virtual Status stepUpIfEligible() override;

    virtual Status abortCatchupIfNeeded() override;

private:
    // Back pointer to the ServiceContext that has started the instance.
    ServiceContext* const _service;
};

}  // namespace repl
}  // namespace mongo
