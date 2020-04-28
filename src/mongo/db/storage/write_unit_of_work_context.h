/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#pragma once

#include <boost/optional.hpp>
#include <memory>

#include "mongo/db/operation_context.h"

namespace mongo {
class WriteUnitOfWorkContext : public Decorable<WriteUnitOfWorkContext> {
public:
    template <class DecorationT>
    static boost::optional<DecorationT&> get(
        OperationContext* opCtx, const WriteUnitOfWorkContext::Decoration<DecorationT>& decoration);
};

class WriteUnitOfWorkContextStorage {
public:
    static const OperationContext::Decoration<WriteUnitOfWorkContextStorage> get;

    void create();
    void discard();
    void restore(std::unique_ptr<WriteUnitOfWorkContext> ctx);
    std::unique_ptr<WriteUnitOfWorkContext> release();

private:
    template <class DecorationT>
    friend boost::optional<DecorationT&> WriteUnitOfWorkContext::get(
        OperationContext* opCtx, const WriteUnitOfWorkContext::Decoration<DecorationT>& decoration);

    std::unique_ptr<WriteUnitOfWorkContext> _context;
};

template <class DecorationT>
boost::optional<DecorationT&> WriteUnitOfWorkContext::get(
    OperationContext* opCtx, const WriteUnitOfWorkContext::Decoration<DecorationT>& decoration) {
    auto& storage = WriteUnitOfWorkContextStorage::get(opCtx);
    if (storage._context) {
        return decoration(storage._context.get());
    } else {
        return boost::none;
    }
}

}  // namespace mongo
