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

#include "mongo/transport/service_executor_winpool.h"

#include "mongo/util/log.h"

namespace mongo {
namespace transport {

	ServiceExecutorWinPool::ServiceExecutorWinPool(ServiceContext* ctx)
{
}


ServiceExecutorWinPool::~ServiceExecutorWinPool() {
    //invariant(!_isRunning.load());
}

Status ServiceExecutorWinPool::start() {
    /*invariant(!_isRunning.load());
    _isRunning.store(true);
    _controllerThread = stdx::thread(&ServiceExecutorAdaptive::_controllerThreadRoutine, this);
    for (auto i = 0; i < _config->reservedThreads(); i++) {
        _startWorkerThread();
    }*/

    return Status::OK();
}

Status ServiceExecutorWinPool::shutdown(Milliseconds timeout) {
    /*if (!_isRunning.load())
        return Status::OK();

    _isRunning.store(false);

    _scheduleCondition.notify_one();
    _controllerThread.join();

    stdx::unique_lock<stdx::mutex> lk(_threadsMutex);
    _ioContext->stop();
    bool result =
        _deathCondition.wait_for(lk, timeout.toSystemDuration(), [&] { return _threads.empty(); });

    return result
        ? Status::OK()
        : Status(ErrorCodes::Error::ExceededTimeLimit,
                 "adaptive executor couldn't shutdown all worker threads within time limit.");*/
	return Status::OK();
}

Status ServiceExecutorWinPool::schedule(ServiceExecutorWinPool::Task task, ScheduleFlags flags) {
    //auto scheduleTime = _tickSource->getTicks();
    //auto pendingCounterPtr = (flags & kDeferredTask) ? &_deferredTasksQueued : &_tasksQueued;
    //pendingCounterPtr->addAndFetch(1);

    //if (!_isRunning.load()) {
    //    return {ErrorCodes::ShutdownInProgress, "Executor is not running"};
    //}

    //auto wrappedTask = [ this, task = std::move(task), scheduleTime, pendingCounterPtr ] {
    //    pendingCounterPtr->subtractAndFetch(1);
    //    auto start = _tickSource->getTicks();
    //    _totalSpentQueued.addAndFetch(start - scheduleTime);
    //    _tasksExecuting.addAndFetch(1);

    //    if (_localThreadState->recursionDepth++ == 0) {
    //        _localThreadState->executing.markRunning();
    //    }
    //    const auto guard = MakeGuard([this, start] {
    //        _tasksExecuting.subtractAndFetch(1);
    //        if (--_localThreadState->recursionDepth == 0) {
    //            _localThreadState->executingCurRun += _localThreadState->executing.markStopped();
    //        }
    //        _totalExecuted.addAndFetch(1);
    //    });

    //    task();
    //};

    //// Dispatching a task on the io_context will run the task immediately, and may run it
    //// on the current thread (if the current thread is running the io_context right now).
    ////
    //// Posting a task on the io_context will run the task without recursion.
    ////
    //// If the task is allowed to recurse and we are not over the depth limit, dispatch it so it
    //// can be called immediately and recursively.
    //if ((flags & kMayRecurse) &&
    //    (_localThreadState->recursionDepth + 1 < _config->recursionLimit())) {
    //    _ioContext->dispatch(std::move(wrappedTask));
    //} else {
    //    _ioContext->post(std::move(wrappedTask));
    //}

    //_lastScheduleTimer.reset();
    //_totalQueued.addAndFetch(1);

    //// Deferred tasks never count against the thread starvation avoidance. For other tasks, we
    //// notify the controller thread that a task has been scheduled and we should monitor thread
    //// starvation.
    //if (_isStarved() && !(flags & kDeferredTask)) {
    //    _scheduleCondition.notify_one();
    //}

	auto t = new ServiceExecutorWinPool::Task(task);

	auto work = CreateThreadpoolWork([](PTP_CALLBACK_INSTANCE Instance, PVOID Param, PTP_WORK work)
	{
		ServiceExecutorWinPool::Task* task = reinterpret_cast<ServiceExecutorWinPool::Task*>(Param);
		(*task)();
		delete task;
	}, t, nullptr);


	SubmitThreadpoolWork(work);

    return Status::OK();
}

void ServiceExecutorWinPool::appendStats(BSONObjBuilder* bob) const {
    
}

}  // namespace transport
}  // namespace mongo
