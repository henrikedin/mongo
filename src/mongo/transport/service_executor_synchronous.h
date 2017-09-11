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

#include <deque>

#include "mongo/base/status.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/transport/service_executor.h"
#include "mongo/util/concurrency/thread_pool_interface.h"

namespace mongo {
namespace transport {

/**
 * The passthrough service executor emulates a thread per connection.
 * Each connection has its own worker thread where jobs get scheduled.
 */
class ServiceExecutorSynchronous final : public ServiceExecutor {
public:
    explicit ServiceExecutorSynchronous(ServiceContext* ctx);
    virtual ~ServiceExecutorSynchronous();

    Status start() override;
    Status shutdown() override;
    Status schedule(Task task, ScheduleFlags flags) override;

    Mode transportMode() const override {
        return Mode::Synchronous;
    }

    void appendStats(BSONObjBuilder* bob) const override;

private:
    static thread_local std::deque<Task> _tlWorkQueue;
    AtomicBool _stillRunning{false};

    mutable stdx::mutex _shutdownMutex;
    stdx::condition_variable _shutdownCondition;

    size_t _numRunningWorkerThreads{0};
};

}  // namespace transport
}  // namespace mongo
