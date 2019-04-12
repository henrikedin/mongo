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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include <cstdlib>

#include "mongo/stdx/mutex.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/signal_handlers_synchronous.h"
#include "mongo/util/log.h"

#if defined(MONGO_USE_GPERFTOOLS_TCMALLOC)
#include <gperftools/tcmalloc.h>
#endif

namespace mongo {

stdx::mutex& alloc_mutex() {
    static stdx::mutex am;
    return am;
}

stdx::unordered_map<void*, size_t>& alloc_map() {
static stdx::unordered_map<void*, size_t> am;
return am;
}

void* mongoMalloc(size_t size) {
#if defined(MONGO_USE_GPERFTOOLS_TCMALLOC)
    void* x = tc_malloc(size);
    {
        stdx::lock_guard lk(alloc_mutex());
        alloc_map()[x] = size;
    }

#else
    void* x = std::malloc(size);
#endif
    if (x == NULL) {
        reportOutOfMemoryErrorAndExit();
    }
    return x;
}

void* mongoRealloc(void* ptr, size_t size) {
#if defined(MONGO_USE_GPERFTOOLS_TCMALLOC)
    {
        stdx::lock_guard lk(alloc_mutex());
        alloc_map().erase(ptr);
    }
    void* x = tc_realloc(ptr, size);
    {
        stdx::lock_guard lk(alloc_mutex());
        alloc_map()[x] = size;
    }
#else
    void* x = std::realloc(ptr, size);
#endif
    if (x == NULL) {
        reportOutOfMemoryErrorAndExit();
    }
    return x;
}

void mongoFree(void* ptr) {
#if defined(MONGO_USE_GPERFTOOLS_TCMALLOC)
    if (ptr) {
        stdx::lock_guard lk(alloc_mutex());
        alloc_map().erase(ptr);
    }
    tc_free(ptr);
#else
    std::free(ptr);
#endif
}

void mongoFree(void* ptr, size_t size) {
#if defined(MONGO_USE_GPERFTOOLS_TCMALLOC)
    size_t alloc_size;
    if (ptr)
    {
        stdx::lock_guard lk(alloc_mutex());
        auto it = alloc_map().find(ptr);
		if (it == alloc_map().end())
		{
            error() << "Trying to free address 0x" << std::hex << ptr << " that we havent allocated?";
            fassert(51179, false);
		}
        alloc_size = it->second;
        fassert(51178, alloc_size == size);
        alloc_map().erase(ptr);
    }

	if (size < 4096) 
		tc_free_sized(ptr, size);
    else
		tc_free(ptr);
#else
    std::free(ptr);
#endif
}

}  // namespace mongo
