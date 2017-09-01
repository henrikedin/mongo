/**
 *    Copyright (C) 2017 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kExecutor;

#include "mongo/platform/basic.h"

#include "mongo/transport/service_entry_point_utils.h"
#include "mongo/transport/service_executor_passthrough.h"

#include "mongo/stdx/chrono.h"
#include "mongo/stdx/thread.h"

#include "mongo/db/server_parameters.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

namespace mongo {
namespace transport {
namespace {
constexpr auto kThreadsRunning = "threadsRunning";
constexpr auto kExecutorLabel = "executor";
constexpr auto kExecutorName = "passthrough";
}  // namespace

thread_local std::deque<ThreadPoolInterface::Task> ServiceExecutorPassthrough::_tlWorkQueue = {};

ServiceExecutorPassthrough::ServiceExecutorPassthrough(ServiceContext* ctx) {}

ServiceExecutorPassthrough::~ServiceExecutorPassthrough() {
    Status status = shutdown();
}

Status ServiceExecutorPassthrough::start() {
    _numHardwareCores = [] {
        ProcessInfo p;
        if (auto availCores = p.getNumAvailableCores()) {
            return static_cast<unsigned>(*availCores);
        }
        return static_cast<unsigned>(p.getNumCores());
    }();

    _stillRunning.store(true);

    return Status::OK();
}

Status ServiceExecutorPassthrough::shutdown() {
    log() << "Shutting down passthrough executor";

    _stillRunning.store(false);

    stdx::unique_lock<stdx::mutex> lock(_shutdownMutex);
    bool result = _shutdownCondition.wait_for(
        lock, stdx::chrono::seconds(10), [this]() { return _numRunningWorkerThreads.load() == 0; });

    return result
        ? Status::OK()
        : Status(ErrorCodes::Error::ExceededTimeLimit,
                 "passthrough executor couldn't shutdown all worker threads within time limit.");
}

Status ServiceExecutorPassthrough::schedule(Task task, ScheduleFlags flags) {
    invariant(_stillRunning.load());

    // As we're running the network in synchronous mode there should always be
    // tasks in the work queue unless this is the first call to schedule for this connection
    if (!_tlWorkQueue.empty()) {
        _tlWorkQueue.emplace_back(std::move(task));
        return Status::OK();
    }

    // First call to schedule() for this connection, spawn a worker thread that will push jobs
    // into the thread local job queue.
    log() << "Starting new executor thread in passthrough mode";

    Status status = launchServiceWorkerThread([ this, task = std::move(task) ] {
        _numRunningWorkerThreads.addAndFetch(1);
        _tlWorkQueue.emplace_back(std::move(task));
        while (!_tlWorkQueue.empty() && _stillRunning.loadRelaxed()) {
            _tlWorkQueue.front()();
            _tlWorkQueue.pop_front();

            /*
            * In perf testing we found that yielding after running a each request produced
            * at 5% performance boost in microbenchmarks if the number of worker threads
            * was greater than the number of available cores.
            */
            if (_numRunningWorkerThreads.loadRelaxed() > _numHardwareCores)
                stdx::this_thread::yield();
        }

        stdx::unique_lock<stdx::mutex> lock(_shutdownMutex);
        _numRunningWorkerThreads.subtractAndFetch(1);
        stdx::notify_all_at_thread_exit(_shutdownCondition, std::move(lock));
    });

    return status;
}

void ServiceExecutorPassthrough::appendStats(BSONObjBuilder* bob) const {
    BSONObjBuilder section(bob->subobjStart("serviceExecutorTaskStats"));
    section << kExecutorLabel << kExecutorName << kThreadsRunning
            << _numRunningWorkerThreads.load();
    section.doneFast();
}

}  // namespace transport
}  // namespace mongo
