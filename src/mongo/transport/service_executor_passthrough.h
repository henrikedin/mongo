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

#pragma once

#include <vector>
#include <deque>
#include <map>
#include <unordered_map>

#include "mongo/base/status.h"
#include "mongo/platform/atomic_word.h"

#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/concurrency/thread_pool_interface.h"

#include "mongo/transport/service_executor.h"

namespace mongo {
namespace transport {

/**
 * The passthrough service executor emulates a thread per connection. 
 * Each connection has its own worker thread where jobs get scheduled.
 */
class ServiceExecutorPassthrough : public ServiceExecutor {
public:
    explicit ServiceExecutorPassthrough(ServiceContext* ctx);
    virtual ~ServiceExecutorPassthrough();

	Status start() final;
	Status shutdown() final;
	Status schedule(Task task, ScheduleFlags flags) final;

	static Mode transportModeStatic() { return Mode::Synchronous; }
	Mode transportMode() const final { return transportModeStatic(); }

	void appendStats(BSONObjBuilder* bob) const final;

private:
	static thread_local std::deque<Task> _workQueue;
	AtomicWord<bool> _stillRunning{ false };

	stdx::mutex _threadsMutex;
	std::unordered_map<stdx::thread::id, stdx::thread> _threads;
	AtomicWord<unsigned> _num_threads{ 0 };
	unsigned _num_cores{ 0 };

};

}  // namespace transport
}  // namespace mongo
