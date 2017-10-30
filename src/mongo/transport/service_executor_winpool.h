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

#include "mongo/db/service_context.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/list.h"
#include "mongo/stdx/thread.h"
#include "mongo/transport/service_executor.h"
#include "mongo/util/tick_source.h"

#include <asio.hpp>

namespace mongo {
namespace transport {

/**
 * This is an ASIO-based adaptive ServiceExecutor. It guarantees that threads will not become stuck
 * or deadlocked longer that its configured timeout and that idle threads will terminate themselves
 * if they spend more than its configure idle threshold idle.
 */
class ServiceExecutorWinPool : public ServiceExecutor {
public:
    explicit ServiceExecutorWinPool(ServiceContext* ctx);

	ServiceExecutorWinPool(ServiceExecutorWinPool&&) = default;
	ServiceExecutorWinPool& operator=(ServiceExecutorWinPool&&) = default;
    virtual ~ServiceExecutorWinPool();

    Status start() final;
    Status shutdown(Milliseconds timeout) final;
    Status schedule(Task task, ScheduleFlags flags) final;

    Mode transportMode() const final {
        return Mode::kAsynchronous;
    }

    void appendStats(BSONObjBuilder* bob) const final;
};

}  // namespace transport
}  // namespace mongo
