/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#include "mongo/db/operation_context.h"

#include <shared_mutex>

namespace mongo {

/**
 * RAII type to safely register a namespace with the CollectionCatalog. Prevents the view catalog
 * from claiming the same namespace concurrently. Throws WriteConflictException if namespace is
 * already taken.
 */
class RegisterNamespaceForCollectionBlock {
public:
    RegisterNamespaceForCollectionBlock(OperationContext* opCtx, const NamespaceString& ns);

private:
    std::shared_lock<std::shared_mutex> _lock;  // NOLINT
};

/**
 * RAII type to safely register a namespace with the ViewCatalog. Prevents the collection catalog
 * from claiming the same namespace concurrently. Throws WriteConflictException if namespace is
 * already taken.
 */
class RegisterNamespaceForViewBlock {
public:
    RegisterNamespaceForViewBlock(OperationContext* opCtx, const NamespaceString& ns);

private:
    std::unique_lock<std::shared_mutex> _lock;  // NOLINT
};

/**
 * For the ViewCatalog to inject implementation if namespace is used by view that
 * 'RegisterNamespaceForCollectionBlock' use to implement its logic without adding a circular link
 * dependency.
 */
void registerNamespaceUsedByViewCatalogFunction(
    std::function<bool(OperationContext*, const NamespaceString&)> impl);

}  // namespace mongo
