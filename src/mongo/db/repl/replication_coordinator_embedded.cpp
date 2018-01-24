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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include "mongo/db/repl/replication_coordinator_embedded.h"
#include "mongo/db/repl/repl_set_config.h"

namespace mongo {
namespace repl {

ReplicationCoordinatorEmbedded::ReplicationCoordinatorEmbedded(
    ServiceContext* service)
	: _service(service) {
}

ReplicationCoordinatorEmbedded::~ReplicationCoordinatorEmbedded() = default;

OpTime ReplicationCoordinatorEmbedded::getCurrentCommittedSnapshotOpTime() const {
	uassert(ErrorCodes::NotImplementedForEmbedded,
		str::stream() << "Not implemented for embedded: ", false);
    return OpTime();
}

void ReplicationCoordinatorEmbedded::appendDiagnosticBSON(mongo::BSONObjBuilder* bob) {
   
}

void ReplicationCoordinatorEmbedded::appendConnectionStats(
    executor::ConnectionPoolStats* stats) const {
}

void ReplicationCoordinatorEmbedded::startup(OperationContext* opCtx) {
}

void ReplicationCoordinatorEmbedded::shutdown(OperationContext* opCtx) {
}

const ReplSettings& ReplicationCoordinatorEmbedded::getSettings() const {
    static ReplSettings _settings;
    return _settings;
}

ReplicationCoordinator::Mode ReplicationCoordinatorEmbedded::getReplicationMode() const {
    return ReplicationCoordinator::Mode::modeNone;
}

MemberState ReplicationCoordinatorEmbedded::getMemberState() const {
    static MemberState _memberState;
    return _memberState;
}

Status ReplicationCoordinatorEmbedded::waitForMemberState(MemberState expectedState,
                                                          Milliseconds timeout) {
    return Status::OK();
}

Seconds ReplicationCoordinatorEmbedded::getSlaveDelaySecs() const {
    return Seconds{0};
}

void ReplicationCoordinatorEmbedded::clearSyncSourceBlacklist() {
}

Status ReplicationCoordinatorEmbedded::setFollowerMode(const MemberState& newState) {
    return Status::OK();
}

ReplicationCoordinator::ApplierState ReplicationCoordinatorEmbedded::getApplierState() {
    static ReplicationCoordinator::ApplierState _applierState;
    return _applierState;
}

void ReplicationCoordinatorEmbedded::signalDrainComplete(OperationContext* opCtx,
                                                         long long termWhenBufferIsEmpty) {
}

Status ReplicationCoordinatorEmbedded::waitForDrainFinish(Milliseconds timeout) {
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::signalUpstreamUpdater() {
}

Status ReplicationCoordinatorEmbedded::setLastOptimeForSlave(const OID& rid, const Timestamp& ts) {
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::setMyHeartbeatMessage(const std::string& msg) {
}

void ReplicationCoordinatorEmbedded::setMyLastAppliedOpTimeForward(const OpTime& opTime,
                                                                   DataConsistency consistency) {
}

void ReplicationCoordinatorEmbedded::setMyLastDurableOpTimeForward(const OpTime& opTime) {
}

void ReplicationCoordinatorEmbedded::setMyLastAppliedOpTime(const OpTime& opTime) {
}

void ReplicationCoordinatorEmbedded::setMyLastDurableOpTime(const OpTime& opTime) {
}

void ReplicationCoordinatorEmbedded::resetMyLastOpTimes() {
}

OpTime ReplicationCoordinatorEmbedded::getMyLastAppliedOpTime() const {
    return OpTime();
}

OpTime ReplicationCoordinatorEmbedded::getMyLastDurableOpTime() const {
    return OpTime();
}

Status ReplicationCoordinatorEmbedded::waitUntilOpTimeForRead(OperationContext* opCtx,
                                                              const ReadConcernArgs& readConcern) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::waitUntilOpTimeForReadUntil(
    OperationContext* opCtx, const ReadConcernArgs& readConcern, boost::optional<Date_t> deadline) {
    return Status::OK();
}

ReplicationCoordinator::StatusAndDuration ReplicationCoordinatorEmbedded::awaitReplication(
    OperationContext* opCtx, const OpTime& opTime, const WriteConcernOptions& writeConcern) {
    return {Status::OK(), Milliseconds(0)};
}

Status ReplicationCoordinatorEmbedded::stepDown(OperationContext* opCtx,
                                                const bool force,
                                                const Milliseconds& waitTime,
                                                const Milliseconds& stepdownTime) {

    return Status::OK();
}

bool ReplicationCoordinatorEmbedded::isMasterForReportingPurposes() {
    return true;
}

bool ReplicationCoordinatorEmbedded::canAcceptWritesForDatabase(OperationContext* opCtx,
                                                                StringData dbName) {
    return true;
}

bool ReplicationCoordinatorEmbedded::canAcceptWritesForDatabase_UNSAFE(OperationContext* opCtx,
                                                                       StringData dbName) {
    return true;
}

bool ReplicationCoordinatorEmbedded::canAcceptWritesFor(OperationContext* opCtx,
                                                        const NamespaceString& ns) {
    return true;
}

bool ReplicationCoordinatorEmbedded::canAcceptWritesFor_UNSAFE(OperationContext* opCtx,
                                                               const NamespaceString& ns) {
    return true;
}

Status ReplicationCoordinatorEmbedded::checkCanServeReadsFor(OperationContext* opCtx,
                                                             const NamespaceString& ns,
                                                             bool slaveOk) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::checkCanServeReadsFor_UNSAFE(OperationContext* opCtx,
                                                                    const NamespaceString& ns,
                                                                    bool slaveOk) {
    return Status::OK();
}

bool ReplicationCoordinatorEmbedded::isInPrimaryOrSecondaryState() const {
    return false;
}

bool ReplicationCoordinatorEmbedded::shouldRelaxIndexConstraints(OperationContext* opCtx,
                                                                 const NamespaceString& ns) {
    return false;
}

OID ReplicationCoordinatorEmbedded::getElectionId() {
    return OID();
}

OID ReplicationCoordinatorEmbedded::getMyRID() const {
    return OID();
}

int ReplicationCoordinatorEmbedded::getMyId() const {
    return 0;
}

Status ReplicationCoordinatorEmbedded::resyncData(OperationContext* opCtx,
                                                  bool waitUntilCompleted) {
    return Status::OK();
}

StatusWith<BSONObj> ReplicationCoordinatorEmbedded::prepareReplSetUpdatePositionCommand() const {
    return BSONObj();
}

Status ReplicationCoordinatorEmbedded::processReplSetGetStatus(
    BSONObjBuilder* response, ReplSetGetStatusResponseStyle responseStyle) {

    return Status::OK();
}

void ReplicationCoordinatorEmbedded::fillIsMasterForReplSet(IsMasterResponse* response) {
}

void ReplicationCoordinatorEmbedded::appendSlaveInfoData(BSONObjBuilder* result) {
}

ReplSetConfig ReplicationCoordinatorEmbedded::getConfig() const {
    static ReplSetConfig _rsConfig;
    return _rsConfig;
}

void ReplicationCoordinatorEmbedded::processReplSetGetConfig(BSONObjBuilder* result) {
}

void ReplicationCoordinatorEmbedded::processReplSetMetadata(
    const rpc::ReplSetMetadata& replMetadata) {
}

void ReplicationCoordinatorEmbedded::cancelAndRescheduleElectionTimeout() {
}

bool ReplicationCoordinatorEmbedded::getMaintenanceMode() {
    return false;
}

Status ReplicationCoordinatorEmbedded::setMaintenanceMode(bool activate) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetSyncFrom(OperationContext* opCtx,
                                                              const HostAndPort& target,
                                                              BSONObjBuilder* resultObj) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetFreeze(int secs, BSONObjBuilder* resultObj) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processHeartbeat(const ReplSetHeartbeatArgs& args,
                                                        ReplSetHeartbeatResponse* response) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetReconfig(OperationContext* opCtx,
                                                              const ReplSetReconfigArgs& args,
                                                              BSONObjBuilder* resultObj) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetInitiate(OperationContext* opCtx,
                                                              const BSONObj& configObj,
                                                              BSONObjBuilder* resultObj) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::abortCatchupIfNeeded() {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetFresh(const ReplSetFreshArgs& args,
                                                           BSONObjBuilder* resultObj) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetElect(const ReplSetElectArgs& args,
                                                           BSONObjBuilder* responseObj) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetUpdatePosition(
    const UpdatePositionArgs& updates, long long* configVersion) {
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processHandshake(OperationContext* opCtx,
                                                        const HandshakeArgs& handshake) {
    return Status::OK();
}

bool ReplicationCoordinatorEmbedded::buildsIndexes() {
    return false;
}

std::vector<HostAndPort> ReplicationCoordinatorEmbedded::getHostsWrittenTo(const OpTime& op,
                                                                           bool durablyWritten) {
    return std::vector<HostAndPort>();
}

std::vector<HostAndPort> ReplicationCoordinatorEmbedded::getOtherNodesInReplSet() const {
    return std::vector<HostAndPort>();
}

Status ReplicationCoordinatorEmbedded::checkIfWriteConcernCanBeSatisfied(
    const WriteConcernOptions& writeConcern) const {
    return Status::OK();
}

WriteConcernOptions ReplicationCoordinatorEmbedded::getGetLastErrorDefault() {
    return WriteConcernOptions();
}

Status ReplicationCoordinatorEmbedded::checkReplEnabledForCommand(BSONObjBuilder* result) {
    return Status::OK();
}

bool ReplicationCoordinatorEmbedded::isReplEnabled() const {
    return false;
}

HostAndPort ReplicationCoordinatorEmbedded::chooseNewSyncSource(const OpTime& lastOpTimeFetched) {
    return HostAndPort();
}

void ReplicationCoordinatorEmbedded::blacklistSyncSource(const HostAndPort& host, Date_t until) {
}

void ReplicationCoordinatorEmbedded::resetLastOpTimesFromOplog(OperationContext* opCtx,
                                                               DataConsistency consistency) {
}

bool ReplicationCoordinatorEmbedded::shouldChangeSyncSource(
    const HostAndPort& currentSource,
    const rpc::ReplSetMetadata& replMetadata,
    boost::optional<rpc::OplogQueryMetadata> oqMetadata) {
    return false;
}

void ReplicationCoordinatorEmbedded::advanceCommitPoint(const OpTime& committedOpTime) {
}

OpTime ReplicationCoordinatorEmbedded::getLastCommittedOpTime() const {
    return OpTime();
}

Status ReplicationCoordinatorEmbedded::processReplSetRequestVotes(
    OperationContext* opCtx,
    const ReplSetRequestVotesArgs& args,
    ReplSetRequestVotesResponse* response) {
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::prepareReplMetadata(OperationContext* opCtx,
                                                         const BSONObj& metadataRequestObj,
                                                         const OpTime& lastOpTimeFromClient,
                                                         BSONObjBuilder* builder) const {

}

bool ReplicationCoordinatorEmbedded::isV1ElectionProtocol() const {
    return false;
}

bool ReplicationCoordinatorEmbedded::getWriteConcernMajorityShouldJournal() {
    return false;
}

Status ReplicationCoordinatorEmbedded::processHeartbeatV1(const ReplSetHeartbeatArgsV1& args,
                                                          ReplSetHeartbeatResponse* response) {
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::summarizeAsHtml(ReplSetHtmlSummary* output) {
}

long long ReplicationCoordinatorEmbedded::getTerm() {
    return 0;
}

Status ReplicationCoordinatorEmbedded::updateTerm(OperationContext* opCtx, long long term) {
    return Status::OK();
}

Timestamp ReplicationCoordinatorEmbedded::getMinimumVisibleSnapshot(OperationContext* opCtx) {
    return Timestamp();
}

void ReplicationCoordinatorEmbedded::waitUntilSnapshotCommitted(OperationContext* opCtx,
                                                                const Timestamp& untilSnapshot) {
}

size_t ReplicationCoordinatorEmbedded::getNumUncommittedSnapshots() {
    return 0;
}

void ReplicationCoordinatorEmbedded::dropAllSnapshots() {
}

WriteConcernOptions ReplicationCoordinatorEmbedded::populateUnsetWriteConcernOptionsSyncMode(
    WriteConcernOptions wc) {
    WriteConcernOptions writeConcern(wc);
    writeConcern.syncMode = WriteConcernOptions::SyncMode::NONE;
    return writeConcern;
}

Status ReplicationCoordinatorEmbedded::stepUpIfEligible() {
    return Status::OK();
}

ReplSettings::IndexPrefetchConfig ReplicationCoordinatorEmbedded::getIndexPrefetchConfig() const {
    return ReplSettings::IndexPrefetchConfig();
}

void ReplicationCoordinatorEmbedded::setIndexPrefetchConfig(
    const ReplSettings::IndexPrefetchConfig cfg) {
}

}  // namespace repl
}  // namespace mongo
