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
#include "mongo/transport/service_executor_synchronous.h"

#include "mongo/stdx/thread.h"

#include "mongo/db/server_parameters.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

namespace mongo {
namespace transport {
namespace {
constexpr auto kThreadsRunning = "threadsRunning"_sd;
constexpr auto kExecutorLabel = "executor"_sd;
constexpr auto kExecutorName = "passthrough"_sd;
}  // namespace

thread_local std::deque<ThreadPoolInterface::Task> ServiceExecutorSynchronous::_localWorkQueue = {};

ServiceExecutorSynchronous::ServiceExecutorSynchronous(ServiceContext* ctx) {}

ServiceExecutorSynchronous::~ServiceExecutorSynchronous() {
    Status status = shutdown();
}

Status ServiceExecutorSynchronous::start() {
    _numHardwareCores = [] {
        ProcessInfo p;
        if (auto availCores = p.getNumAvailableCores()) {
            return static_cast<size_t>(*availCores);
        }
        return static_cast<size_t>(p.getNumCores());
    }();

    _stillRunning.store(true);

    return Status::OK();
}

Status ServiceExecutorSynchronous::shutdown() {
    LOG(3) << "Shutting down passthrough executor";

    _stillRunning.store(false);

    // TODO pass a time into this function
    stdx::unique_lock<stdx::mutex> lock(_shutdownMutex);
    bool result = _shutdownCondition.wait_for(lock, Seconds(10).toSystemDuration(), [this]() {
        return _numRunningWorkerThreads.load() == 0;
    });

    return result
        ? Status::OK()
        : Status(ErrorCodes::Error::ExceededTimeLimit,
                 "passthrough executor couldn't shutdown all worker threads within time limit.");
}

Status ServiceExecutorSynchronous::schedule(Task task,
                                            ScheduleFlags flags,
                                            ServiceStateMachineState state) {
    // If we have a positive recursion depth we're already running in a worker thread, determine if
    // we can recurse deeper (estimate of remaining stack space), otherwise unroll and queue for the
    // loop in the thread.
    if (!_localWorkQueue.empty()) {
        if (state == ServiceStateMachineState::Process) {
            task();
        } else {
            _localWorkQueue.emplace_back(std::move(task));
        }
        return Status::OK();
    }

    // First call to schedule() for this connection, spawn a worker thread that will push jobs
    // into the thread local job queue.
    LOG(3) << "Starting new executor thread in passthrough mode";

    Status status = launchServiceWorkerThread([ this, task = std::move(task) ] {
        _numRunningWorkerThreads.addAndFetch(1);

        _localWorkQueue.emplace_back(std::move(task));
        while (!_localWorkQueue.empty() && _stillRunning.loadRelaxed()) {
            _localWorkQueue.front()();
            _localWorkQueue.pop_front();

            /*
            * In perf testing we found that yielding after running a each request produced
            * at 5% performance boost in microbenchmarks if the number of worker threads
            * was greater than the number of available cores.
            */
            if (_numRunningWorkerThreads.loadRelaxed() > _numHardwareCores)
                stdx::this_thread::yield();
        }

        if (_numRunningWorkerThreads.subtractAndFetch(1) == 0) {
            stdx::unique_lock<stdx::mutex> lock(_shutdownMutex);
            stdx::notify_all_at_thread_exit(_shutdownCondition, std::move(lock));
        }
    });

    return status;
}

void ServiceExecutorSynchronous::appendStats(BSONObjBuilder* bob) const {
    BSONObjBuilder section(bob->subobjStart("serviceExecutorTaskStats"));
    section << kExecutorLabel << kExecutorName << kThreadsRunning
            << (int)_numRunningWorkerThreads.loadRelaxed();
}

}  // namespace transport
}  // namespace mongo
