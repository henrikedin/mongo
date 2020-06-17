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
#pragma once

#include "mongo/db/index/index_descriptor_fwd.h"
#include "mongo/db/storage/biggie/store.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/sorted_data_interface.h"

namespace mongo {
namespace biggie {

class SortedDataBuilderBase : public ::mongo::SortedDataBuilderInterface {
public:
    SortedDataBuilderBase(OperationContext* opCtx,
                          bool dupsAllowed,
                          Ordering order,
                          const std::string& prefix,
                          const std::string& identEnd,
                          const NamespaceString& collectionNamespace,
                          const std::string& indexName,
                          const BSONObj& keyPattern,
                          const BSONObj& collation);
    void commit(bool mayInterrupt) override;

protected:
    OperationContext* _opCtx;
    bool _dupsAllowed;
    // Order of the keys.
    Ordering _order;
    // Prefix and identEnd for the ident.
    std::string _prefix;
    std::string _identEnd;
    // Index metadata.
    const NamespaceString _collectionNamespace;
    const std::string _indexName;
    const BSONObj _keyPattern;
    const BSONObj _collation;
};

class SortedDataBuilderUnique : public SortedDataBuilderBase {
public:
    using SortedDataBuilderBase::SortedDataBuilderBase;
    virtual Status addKey(const KeyString::Value& keyString);
};

class SortedDataInterfaceBase : public ::mongo::SortedDataInterface {
public:
    // Truncate is not required at the time of writing but will be when the truncate command is
    // created
    Status truncate(RecoveryUnit* ru);
    SortedDataInterfaceBase(OperationContext* opCtx, StringData ident, const IndexDescriptor* desc);
    SortedDataInterfaceBase(const Ordering& ordering, StringData ident);
    virtual bool appendCustomStats(OperationContext* opCtx,
                                   BSONObjBuilder* output,
                                   double scale) const override;
    virtual long long getSpaceUsedBytes(OperationContext* opCtx) const override;
    virtual bool isEmpty(OperationContext* opCtx) override;
    virtual Status initAsEmpty(OperationContext* opCtx) override;

protected:
    // These two are the same as before.
    std::string _prefix;
    std::string _identEnd;
    // Index metadata.
    const NamespaceString _collectionNamespace;
    const std::string _indexName;
    const BSONObj _keyPattern;
    const BSONObj _collation;
    // These are the keystring representations of the _prefix and the _identEnd.
    std::string _KSForIdentStart;
    std::string _KSForIdentEnd;
    // Whether or not the index is partial
    bool _isPartial;
};

class SortedDataInterfaceUnique : public SortedDataInterfaceBase {
public:
    SortedDataInterfaceUnique(OperationContext* opCtx,
                              StringData ident,
                              const IndexDescriptor* desc);
    SortedDataInterfaceUnique(const Ordering& ordering, StringData ident);
    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx,
                                                       bool dupsAllowed) override;
    virtual Status insert(OperationContext* opCtx,
                          const KeyString::Value& keyString,
                          bool dupsAllowed) override;
    virtual void unindex(OperationContext* opCtx,
                         const KeyString::Value& keyString,
                         bool dupsAllowed) override;
    virtual Status dupKeyCheck(OperationContext* opCtx, const KeyString::Value& keyString) override;
    virtual void fullValidate(OperationContext* opCtx,
                              long long* numKeysOut,
                              ValidateResults* fullResults) const override;
    virtual std::unique_ptr<mongo::SortedDataInterface::Cursor> newCursor(
        OperationContext* opCtx, bool isForward = true) const override;

};

class SortedDataBuilderStandard : public SortedDataBuilderBase {
public:
    using SortedDataBuilderBase::SortedDataBuilderBase;
    virtual Status addKey(const KeyString::Value& keyString);
};

class SortedDataInterfaceStandard : public SortedDataInterfaceBase {
public:
    SortedDataInterfaceStandard(OperationContext* opCtx,
                                StringData ident,
                                const IndexDescriptor* desc);
    SortedDataInterfaceStandard(const Ordering& ordering, StringData ident);
    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* opCtx,
                                                       bool dupsAllowed) override;
    virtual Status insert(OperationContext* opCtx,
                          const KeyString::Value& keyString,
                          bool dupsAllowed) override;
    virtual void unindex(OperationContext* opCtx,
                         const KeyString::Value& keyString,
                         bool dupsAllowed) override;
    virtual Status dupKeyCheck(OperationContext* opCtx, const KeyString::Value& keyString) override;
    virtual void fullValidate(OperationContext* opCtx,
                              long long* numKeysOut,
                              ValidateResults* fullResults) const override;
    virtual std::unique_ptr<mongo::SortedDataInterface::Cursor> newCursor(
        OperationContext* opCtx, bool isForward = true) const override;
};
}  // namespace biggie
}  // namespace mongo
