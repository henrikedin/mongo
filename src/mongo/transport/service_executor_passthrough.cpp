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

#include "mongo/transport/service_executor_passthrough.h"

#include "mongo/db/server_parameters.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"

#include <asio.hpp>

namespace mongo {
namespace transport {
namespace {
//MONGO_EXPORT_STARTUP_SERVER_PARAMETER(fixedServiceExecutorNumThreads, int, -1);

}  // namespace

ServiceExecutorPassthrough::ServiceExecutorPassthrough(ServiceContext* ctx,
                                           std::shared_ptr<asio::io_context> ioCtx) {
	_io_context = stdx::make_unique<asio::io_context>();
}

ServiceExecutorPassthrough::~ServiceExecutorPassthrough() {
    //invariant(!_isRunning.load());
}

Status ServiceExecutorPassthrough::start() {
    /*invariant(!_isRunning.load());

    auto threadCount = fixedServiceExecutorNumThreads;
    if (threadCount == -1) {
        ProcessInfo pi;
        threadCount = pi.getNumAvailableCores().value_or(pi.getNumCores());
        log() << "No thread count configured for fixed executor. Using number of cores: "
              << threadCount;
    }

    _isRunning.store(true);
    for (auto i = 0; i < threadCount; i++) {
        _threads.push_back(stdx::thread([this, i] {
            auto threadId = i + 1;
            LOG(3) << "Starting worker thread " << threadId;
            asio::io_context::work work(*_ioContext);
            while (_isRunning.load()) {
                _ioContext->run();
            }
            LOG(3) << "Exiting worker thread " << threadId;
        }));
    }*/

    return Status::OK();
}

Status ServiceExecutorPassthrough::shutdown() {
    /*invariant(_isRunning.load());

    _isRunning.store(false);
    _ioContext->stop();
    for (auto& thread : _threads) {
        thread.join();
    }
    _threads.clear();*/

    return Status::OK();
}

Status ServiceExecutorPassthrough::schedule(Task task, ScheduleFlags flags) {
	task();

	/*auto in_stack = std::make_shared<bool>(false);
	_io_context->dispatch([in_stack, task = std::move(task)]()
	{
		*in_stack = true;
		task();
	});

	if (*in_stack == false)
	{
		stdx::thread([this]()
		{
			_io_context->run_one();
		}).detach();
	}*/

    return Status::OK();
}

void ServiceExecutorPassthrough::appendStats(BSONObjBuilder* bob) const {
	//BSONObjBuilder section(bob->subobjStart("serviceExecutorTaskStats"));
	//section << kExecutorLabel << kExecutorName  //
	//	<< kTotalQueued << _totalQueued.load() << kTotalExecuted << _totalExecuted.load()
	//	<< kTasksQueued << _tasksQueued.load() << kTasksExecuting << _tasksExecuting.load()
	//	<< kTotalTimeRunningUs << ticksToMicros(_totalSpentRunning.load(), _tickSource)
	//	<< kTotalTimeExecutingUs << ticksToMicros(_totalSpentExecuting.load(), _tickSource)
	//	<< kTotalTimeQueuedUs << ticksToMicros(_totalSpentScheduled.load(), _tickSource)
	//	<< kThreadsRunning << _threadsRunning.load() << kThreadsPending
	//	<< _threadsPending.load();
	//section.doneFast();
}

}  // namespace transport
}  // namespace mongo
