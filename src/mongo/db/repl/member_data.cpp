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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include <climits>

#include "mongo/db/repl/member_data.h"
#include "mongo/db/repl/rslog.h"
#include "mongo/logv2/log.h"

namespace mongo {
namespace repl {

MemberData::MemberData() : _health(-1), _authIssue(false), _configIndex(-1), _isSelf(false) {
    _lastResponse.setState(MemberState::RS_UNKNOWN);
    _lastResponse.setElectionTime(Timestamp());
    _lastResponse.setAppliedOpTimeAndWallTime(OpTimeAndWallTime());
}

bool MemberData::setUpValues(Date_t now, ReplSetHeartbeatResponse&& hbResponse) {
    _health = 1;
    if (_upSince == Date_t()) {
        _upSince = now;
    }
    _authIssue = false;
    _lastHeartbeat = now;
    _lastUpdate = now;
    _lastUpdateStale = false;
    _updatedSinceRestart = true;
    _lastHeartbeatMessage.clear();

    if (!hbResponse.hasState()) {
        hbResponse.setState(MemberState::RS_UNKNOWN);
    }
    if (!hbResponse.hasElectionTime()) {
        hbResponse.setElectionTime(_lastResponse.getElectionTime());
    }
    if (!hbResponse.hasAppliedOpTime()) {
        hbResponse.setAppliedOpTimeAndWallTime(_lastResponse.getAppliedOpTimeAndWallTime());
    }
    // Log if the state changes
    if (_lastResponse.getState() != hbResponse.getState()) {
        LOGV2_OPTIONS(21215,
                      {logv2::LogTag::kRS},
                      "Member {hostAndPort} is now in state {hbResponse_getState}",
                      "hostAndPort"_attr = _hostAndPort.toString(),
                      "hbResponse_getState"_attr = hbResponse.getState().toString());
    }

    bool opTimeAdvanced =
        advanceLastAppliedOpTimeAndWallTime(hbResponse.getAppliedOpTimeAndWallTime(), now);
    auto durableOpTimeAndWallTime = hbResponse.hasDurableOpTime()
        ? hbResponse.getDurableOpTimeAndWallTime()
        : OpTimeAndWallTime();
    opTimeAdvanced =
        advanceLastDurableOpTimeAndWallTime(durableOpTimeAndWallTime, now) || opTimeAdvanced;
    _lastResponse = std::move(hbResponse);
    return opTimeAdvanced;
}

void MemberData::setDownValues(Date_t now, const std::string& heartbeatMessage) {
    _health = 0;
    _upSince = Date_t();
    _lastHeartbeat = now;
    _authIssue = false;
    _updatedSinceRestart = true;
    _lastHeartbeatMessage = heartbeatMessage;

    if (_lastResponse.getState() != MemberState::RS_DOWN) {
        LOGV2_OPTIONS(21216,
                      {logv2::LogTag::kRS},
                      "Member {hostAndPort} is now in state RS_DOWN - {heartbeatMessage}",
                      "hostAndPort"_attr = _hostAndPort.toString(),
                      "heartbeatMessage"_attr = redact(heartbeatMessage));
    }

    _lastResponse = ReplSetHeartbeatResponse();
    _lastResponse.setState(MemberState::RS_DOWN);
    _lastResponse.setElectionTime(Timestamp());
    _lastResponse.setAppliedOpTimeAndWallTime(OpTimeAndWallTime());
    _lastResponse.setSyncingTo(HostAndPort());

    // The _lastAppliedOpTime/_lastDurableOpTime fields don't get cleared merely by missing a
    // heartbeat.
}

void MemberData::setAuthIssue(Date_t now) {
    _health = 0;  // set health to 0 so that this doesn't count towards majority.
    _upSince = Date_t();
    _lastHeartbeat = now;
    _authIssue = true;
    _updatedSinceRestart = true;
    _lastHeartbeatMessage.clear();

    if (_lastResponse.getState() != MemberState::RS_UNKNOWN) {
        LOGV2_OPTIONS(
            21217,
            {logv2::LogTag::kRS},
            "Member {hostAndPort} is now in state RS_UNKNOWN due to authentication issue.",
            "hostAndPort"_attr = _hostAndPort.toString());
    }

    _lastResponse = ReplSetHeartbeatResponse();
    _lastResponse.setState(MemberState::RS_UNKNOWN);
    _lastResponse.setElectionTime(Timestamp());
    _lastResponse.setAppliedOpTimeAndWallTime(OpTimeAndWallTime());
    _lastResponse.setSyncingTo(HostAndPort());
}

void MemberData::setLastAppliedOpTimeAndWallTime(OpTimeAndWallTime opTime, Date_t now) {
    invariant(opTime.opTime.isNull() || opTime.wallTime > Date_t());
    _lastUpdate = now;
    _lastUpdateStale = false;
    _lastAppliedOpTime = opTime.opTime;
    _lastAppliedWallTime = opTime.wallTime;
}

void MemberData::setLastDurableOpTimeAndWallTime(OpTimeAndWallTime opTime, Date_t now) {
    invariant(opTime.opTime.isNull() || opTime.wallTime > Date_t());
    _lastUpdate = now;
    _lastUpdateStale = false;
    if (_lastAppliedOpTime < opTime.opTime) {
        // TODO(russotto): We think this should never happen, rollback or no rollback.  Make this an
        // invariant and see what happens.
        LOGV2(21218,
              "Durable progress ({opTime_opTime}) is ahead of the applied progress "
              "({lastAppliedOpTime}. This is likely due to a "
              "rollback. memberid: {memberId}{hostAndPort} previous durable progress: "
              "{lastDurableOpTime}",
              "opTime_opTime"_attr = opTime.opTime,
              "lastAppliedOpTime"_attr = _lastAppliedOpTime,
              "memberId"_attr = _memberId,
              "hostAndPort"_attr = _hostAndPort.toString(),
              "lastDurableOpTime"_attr = _lastDurableOpTime);
    } else {
        _lastDurableOpTime = opTime.opTime;
        _lastDurableWallTime = opTime.wallTime;
    }
}

bool MemberData::advanceLastAppliedOpTimeAndWallTime(OpTimeAndWallTime opTime, Date_t now) {
    invariant(opTime.opTime.isNull() || opTime.wallTime > Date_t());
    _lastUpdate = now;
    _lastUpdateStale = false;
    if (_lastAppliedOpTime < opTime.opTime) {
        setLastAppliedOpTimeAndWallTime(opTime, now);
        return true;
    }
    return false;
}

bool MemberData::advanceLastDurableOpTimeAndWallTime(OpTimeAndWallTime opTime, Date_t now) {
    invariant(opTime.opTime.isNull() || opTime.wallTime > Date_t());
    _lastUpdate = now;
    _lastUpdateStale = false;
    if (_lastDurableOpTime < opTime.opTime) {
        _lastDurableOpTime = opTime.opTime;
        _lastDurableWallTime = opTime.wallTime;
        return true;
    }
    return false;
}

}  // namespace repl
}  // namespace mongo
