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
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getCurrentCommittedSnapshotOpTime", false);
    return OpTime();
}

void ReplicationCoordinatorEmbedded::appendDiagnosticBSON(mongo::BSONObjBuilder* bob) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: appendDiagnosticBSON", false);
}

void ReplicationCoordinatorEmbedded::appendConnectionStats(
    executor::ConnectionPoolStats* stats) const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: appendConnectionStats", false);
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
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getMemberState", false);
    return MemberState();
}

Status ReplicationCoordinatorEmbedded::waitForMemberState(MemberState expectedState,
                                                          Milliseconds timeout) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: waitForMemberState", false);
    return Status::OK();
}

Seconds ReplicationCoordinatorEmbedded::getSlaveDelaySecs() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getSlaveDelaySecs", false);
    return Seconds{0};
}

void ReplicationCoordinatorEmbedded::clearSyncSourceBlacklist() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: clearSyncSourceBlacklist", false);
}

Status ReplicationCoordinatorEmbedded::setFollowerMode(const MemberState& newState) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setFollowerMode", false);
    return Status::OK();
}

ReplicationCoordinator::ApplierState ReplicationCoordinatorEmbedded::getApplierState() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getApplierState", false);
    return ReplicationCoordinator::ApplierState();
}

void ReplicationCoordinatorEmbedded::signalDrainComplete(OperationContext* opCtx,
                                                         long long termWhenBufferIsEmpty) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: signalDrainComplete", false);
}

Status ReplicationCoordinatorEmbedded::waitForDrainFinish(Milliseconds timeout) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: waitForDrainFinish", false);
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::signalUpstreamUpdater() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: signalUpstreamUpdater", false);
}

Status ReplicationCoordinatorEmbedded::setLastOptimeForSlave(const OID& rid, const Timestamp& ts) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setLastOptimeForSlave", false);
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::setMyHeartbeatMessage(const std::string& msg) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setMyHeartbeatMessage", false);
}

void ReplicationCoordinatorEmbedded::setMyLastAppliedOpTimeForward(const OpTime& opTime,
                                                                   DataConsistency consistency) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setMyLastAppliedOpTimeForward", false);
}

void ReplicationCoordinatorEmbedded::setMyLastDurableOpTimeForward(const OpTime& opTime) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setMyLastDurableOpTimeForward", false);
}

void ReplicationCoordinatorEmbedded::setMyLastAppliedOpTime(const OpTime& opTime) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setMyLastAppliedOpTime", false);
}

void ReplicationCoordinatorEmbedded::setMyLastDurableOpTime(const OpTime& opTime) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setMyLastDurableOpTime", false);
}

void ReplicationCoordinatorEmbedded::resetMyLastOpTimes() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: resetMyLastOpTimes", false);
}

OpTime ReplicationCoordinatorEmbedded::getMyLastAppliedOpTime() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getMyLastAppliedOpTime", false);
    return OpTime();
}

OpTime ReplicationCoordinatorEmbedded::getMyLastDurableOpTime() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getMyLastDurableOpTime", false);
    return OpTime();
}

Status ReplicationCoordinatorEmbedded::waitUntilOpTimeForRead(OperationContext* opCtx,
                                                              const ReadConcernArgs& readConcern) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: waitUntilOpTimeForRead", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::waitUntilOpTimeForReadUntil(
    OperationContext* opCtx, const ReadConcernArgs& readConcern, boost::optional<Date_t> deadline) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: waitUntilOpTimeForReadUntil", false);
    return Status::OK();
}

ReplicationCoordinator::StatusAndDuration ReplicationCoordinatorEmbedded::awaitReplication(
    OperationContext* opCtx, const OpTime& opTime, const WriteConcernOptions& writeConcern) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: awaitReplication", false);
    return {Status::OK(), Milliseconds(0)};
}

Status ReplicationCoordinatorEmbedded::stepDown(OperationContext* opCtx,
                                                const bool force,
                                                const Milliseconds& waitTime,
                                                const Milliseconds& stepdownTime) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: stepDown", false);
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
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getElectionId", false);
    return OID();
}

OID ReplicationCoordinatorEmbedded::getMyRID() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getMyRID", false);
    return OID();
}

int ReplicationCoordinatorEmbedded::getMyId() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getMyId", false);
    return 0;
}

Status ReplicationCoordinatorEmbedded::resyncData(OperationContext* opCtx,
                                                  bool waitUntilCompleted) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: resyncData", false);
    return Status::OK();
}

StatusWith<BSONObj> ReplicationCoordinatorEmbedded::prepareReplSetUpdatePositionCommand() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: prepareReplSetUpdatePositionCommand", false);
    return BSONObj();
}

Status ReplicationCoordinatorEmbedded::processReplSetGetStatus(
    BSONObjBuilder* response, ReplSetGetStatusResponseStyle responseStyle) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetGetStatus", false);
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::fillIsMasterForReplSet(IsMasterResponse* response) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: fillIsMasterForReplSet", false);
}

void ReplicationCoordinatorEmbedded::appendSlaveInfoData(BSONObjBuilder* result) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: appendSlaveInfoData", false);
}

ReplSetConfig ReplicationCoordinatorEmbedded::getConfig() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getConfig", false);
    return ReplSetConfig();
}

void ReplicationCoordinatorEmbedded::processReplSetGetConfig(BSONObjBuilder* result) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetGetConfig", false);
}

void ReplicationCoordinatorEmbedded::processReplSetMetadata(
    const rpc::ReplSetMetadata& replMetadata) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetMetadata", false);
}

void ReplicationCoordinatorEmbedded::cancelAndRescheduleElectionTimeout() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: cancelAndRescheduleElectionTimeout", false);
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
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetSyncFrom", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetFreeze(int secs, BSONObjBuilder* resultObj) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetFreeze", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processHeartbeat(const ReplSetHeartbeatArgs& args,
                                                        ReplSetHeartbeatResponse* response) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processHeartbeat", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetReconfig(OperationContext* opCtx,
                                                              const ReplSetReconfigArgs& args,
                                                              BSONObjBuilder* resultObj) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetReconfig", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetInitiate(OperationContext* opCtx,
                                                              const BSONObj& configObj,
                                                              BSONObjBuilder* resultObj) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetInitiate", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::abortCatchupIfNeeded() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: abortCatchupIfNeeded", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetFresh(const ReplSetFreshArgs& args,
                                                           BSONObjBuilder* resultObj) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetFresh", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetElect(const ReplSetElectArgs& args,
                                                           BSONObjBuilder* responseObj) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetElect", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processReplSetUpdatePosition(
    const UpdatePositionArgs& updates, long long* configVersion) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetUpdatePosition", false);
    return Status::OK();
}

Status ReplicationCoordinatorEmbedded::processHandshake(OperationContext* opCtx,
                                                        const HandshakeArgs& handshake) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processHandshake", false);
    return Status::OK();
}

bool ReplicationCoordinatorEmbedded::buildsIndexes() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: buildsIndexes", false);
    return false;
}

std::vector<HostAndPort> ReplicationCoordinatorEmbedded::getHostsWrittenTo(const OpTime& op,
                                                                           bool durablyWritten) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getHostsWrittenTo", false);
    return std::vector<HostAndPort>();
}

std::vector<HostAndPort> ReplicationCoordinatorEmbedded::getOtherNodesInReplSet() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getOtherNodesInReplSet", false);
    return std::vector<HostAndPort>();
}

Status ReplicationCoordinatorEmbedded::checkIfWriteConcernCanBeSatisfied(
    const WriteConcernOptions& writeConcern) const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: checkIfWriteConcernCanBeSatisfied", false);
    return Status::OK();
}

WriteConcernOptions ReplicationCoordinatorEmbedded::getGetLastErrorDefault() {
    return WriteConcernOptions();
}

Status ReplicationCoordinatorEmbedded::checkReplEnabledForCommand(BSONObjBuilder* result) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: checkReplEnabledForCommand", false);
    return Status::OK();
}

bool ReplicationCoordinatorEmbedded::isReplEnabled() const {
    return false;
}

HostAndPort ReplicationCoordinatorEmbedded::chooseNewSyncSource(const OpTime& lastOpTimeFetched) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: chooseNewSyncSource", false);
    return HostAndPort();
}

void ReplicationCoordinatorEmbedded::blacklistSyncSource(const HostAndPort& host, Date_t until) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: blacklistSyncSource", false);
}

void ReplicationCoordinatorEmbedded::resetLastOpTimesFromOplog(OperationContext* opCtx,
                                                               DataConsistency consistency) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: resetLastOpTimesFromOplog", false);
}

bool ReplicationCoordinatorEmbedded::shouldChangeSyncSource(
    const HostAndPort& currentSource,
    const rpc::ReplSetMetadata& replMetadata,
    boost::optional<rpc::OplogQueryMetadata> oqMetadata) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: shouldChangeSyncSource", false);
    return false;
}

void ReplicationCoordinatorEmbedded::advanceCommitPoint(const OpTime& committedOpTime) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: advanceCommitPoint", false);
}

OpTime ReplicationCoordinatorEmbedded::getLastCommittedOpTime() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getLastCommittedOpTime", false);
    return OpTime();
}

Status ReplicationCoordinatorEmbedded::processReplSetRequestVotes(
    OperationContext* opCtx,
    const ReplSetRequestVotesArgs& args,
    ReplSetRequestVotesResponse* response) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processReplSetRequestVotes", false);
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::prepareReplMetadata(OperationContext* opCtx,
                                                         const BSONObj& metadataRequestObj,
                                                         const OpTime& lastOpTimeFromClient,
                                                         BSONObjBuilder* builder) const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: prepareReplMetadata", false);
}

bool ReplicationCoordinatorEmbedded::isV1ElectionProtocol() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: isV1ElectionProtocol", false);
    return false;
}

bool ReplicationCoordinatorEmbedded::getWriteConcernMajorityShouldJournal() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getWriteConcernMajorityShouldJournal", false);
    return false;
}

Status ReplicationCoordinatorEmbedded::processHeartbeatV1(const ReplSetHeartbeatArgsV1& args,
                                                          ReplSetHeartbeatResponse* response) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: processHeartbeatV1", false);
    return Status::OK();
}

void ReplicationCoordinatorEmbedded::summarizeAsHtml(ReplSetHtmlSummary* output) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: summarizeAsHtml", false);
}

long long ReplicationCoordinatorEmbedded::getTerm() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getTerm", false);
    return 0;
}

Status ReplicationCoordinatorEmbedded::updateTerm(OperationContext* opCtx, long long term) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: updateTerm", false);
    return Status::OK();
}

Timestamp ReplicationCoordinatorEmbedded::getMinimumVisibleSnapshot(OperationContext* opCtx) {
    return Timestamp();
}

void ReplicationCoordinatorEmbedded::waitUntilSnapshotCommitted(OperationContext* opCtx,
                                                                const Timestamp& untilSnapshot) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: waitUntilSnapshotCommitted", false);
}

size_t ReplicationCoordinatorEmbedded::getNumUncommittedSnapshots() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getNumUncommittedSnapshots", false);
    return 0;
}

void ReplicationCoordinatorEmbedded::dropAllSnapshots() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: dropAllSnapshots", false);
}

WriteConcernOptions ReplicationCoordinatorEmbedded::populateUnsetWriteConcernOptionsSyncMode(
    WriteConcernOptions wc) {
    WriteConcernOptions writeConcern(wc);
    writeConcern.syncMode = WriteConcernOptions::SyncMode::NONE;
    return writeConcern;
}

Status ReplicationCoordinatorEmbedded::stepUpIfEligible() {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: stepUpIfEligible", false);
    return Status::OK();
}

ReplSettings::IndexPrefetchConfig ReplicationCoordinatorEmbedded::getIndexPrefetchConfig() const {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: getIndexPrefetchConfig", false);
    return ReplSettings::IndexPrefetchConfig();
}

void ReplicationCoordinatorEmbedded::setIndexPrefetchConfig(
    const ReplSettings::IndexPrefetchConfig cfg) {
	uassert(ErrorCodes::NotImplementedForEmbedded, "Not implemented for embedded: setIndexPrefetchConfig", false);
}

}  // namespace repl
}  // namespace mongo
