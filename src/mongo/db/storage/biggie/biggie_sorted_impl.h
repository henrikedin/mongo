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

#include <boost/iterator/iterator_facade.hpp>
#include <map>

namespace mongo {
namespace biggie {

class IndexDataEntry {
public:
    IndexDataEntry() : _buffer(nullptr) {}
    IndexDataEntry(const uint8_t* buffer);
    IndexDataEntry(const std::string& indexDataEntry);
    
    const uint8_t* buffer() const;
    size_t size() const;
    RecordId loc() const;
    KeyString::TypeBits typeBits() const;

private:
    const uint8_t* _buffer;
};

class IndexDataEntryIterator : public boost::iterator_facade<IndexDataEntryIterator,
                                                             IndexDataEntry const,
                                                             boost::forward_traversal_tag> {
public:
    IndexDataEntryIterator() = default;
    IndexDataEntryIterator(const uint8_t* entry);

private:
    friend class boost::iterator_core_access;

    void increment();
    bool equal(IndexDataEntryIterator const& other) const;
    const IndexDataEntry& dereference() const;

    IndexDataEntry _entry;
};

class IndexData {
public:
    IndexData() : _begin(nullptr), _end(nullptr), _size(0) {}
    IndexData(const std::string& indexData) {
        _begin = reinterpret_cast<const uint8_t*>(indexData.data() + sizeof(uint64_t));
        _end = reinterpret_cast<const uint8_t*>(indexData.data() + indexData.size());
        std::memcpy(&_size, indexData.data(), sizeof(uint64_t));
    }

    using const_iterator = IndexDataEntryIterator;

    size_t size() const {
        return _size;
    }
    bool empty() const {
        return _size == 0;
    }
    const_iterator begin() const {
        return IndexDataEntryIterator(_begin);
    }
    const_iterator end() const {
        return IndexDataEntryIterator(_end);
    }

    const_iterator lower_bound(RecordId loc) const;
    const_iterator upper_bound(RecordId loc) const;

    boost::optional<std::string> add(RecordId loc, KeyString::TypeBits typeBits);
    boost::optional<std::string> remove(RecordId loc);

private:
    const uint8_t* _begin;
    const uint8_t* _end;
    size_t _size;
};

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
    SortedDataInterfaceBase(OperationContext* opCtx,
                              StringData ident,
                              const IndexDescriptor* desc);
    SortedDataInterfaceBase(const Ordering& ordering, StringData ident);
    virtual bool appendCustomStats(OperationContext* opCtx,
                                   BSONObjBuilder* output,
                                   double scale) const override;
    virtual long long getSpaceUsedBytes(OperationContext* opCtx) const override;
    virtual bool isEmpty(OperationContext* opCtx) override;
    virtual Status initAsEmpty(OperationContext* opCtx) override;

    ///*
    // * This is the cursor class required by the sorted data interface.
    // */
    //class Cursor final : public ::mongo::SortedDataInterface::Cursor {
    //public:
    //    // All the following public functions just implement the interface.
    //    Cursor(OperationContext* opCtx,
    //           bool isForward,
    //           // This is the ident.
    //           std::string _prefix,
    //           // This is a string immediately after the ident and before other idents.
    //           std::string _identEnd,
    //           StringStore* workingCopy,
    //           Ordering order,
    //           std::string prefixBSON,
    //           std::string KSForIdentEnd);
    //    virtual void setEndPosition(const BSONObj& key, bool inclusive) override;
    //    virtual boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) override;
    //    virtual boost::optional<KeyStringEntry> nextKeyString() override;
    //    virtual boost::optional<IndexKeyEntry> seek(const KeyString::Value& keyString,
    //                                                RequestedInfo parts = kKeyAndLoc) override;
    //    virtual boost::optional<KeyStringEntry> seekForKeyString(
    //        const KeyString::Value& keyStringValue) override;
    //    virtual boost::optional<KeyStringEntry> seekExactForKeyString(
    //        const KeyString::Value& keyStringValue) override;
    //    virtual boost::optional<IndexKeyEntry> seekExact(const KeyString::Value& keyStringValue,
    //                                                     RequestedInfo) override;
    //    virtual void save() override;
    //    virtual void restore() override;
    //    virtual void detachFromOperationContext() override;
    //    virtual void reattachToOperationContext(OperationContext* opCtx) override;

    //private:
    //    bool advanceNext();
    //    // This is a helper function to check if the cursor was explicitly set by the user or not.
    //    bool endPosSet();
    //    // This is a helper function to check if the cursor is valid or not.
    //    bool checkCursorValid();
    //    // Helper function to set index data iterators to reverse position
    //    void initReverseDataIterators();
    //    // This is a helper function for seek.
    //    boost::optional<IndexKeyEntry> seekAfterProcessing(BSONObj finalKey);
    //    boost::optional<KeyStringEntry> seekAfterProcessing(const KeyString::Value& keyString);
    //    OperationContext* _opCtx;
    //    // This is the "working copy" of the master "branch" in the git analogy.
    //    StringStore* _workingCopy;
    //    // These store the end positions.
    //    boost::optional<StringStore::const_iterator> _endPos;
    //    boost::optional<StringStore::const_reverse_iterator> _endPosReverse;
    //    // This means if the cursor is a forward or reverse cursor.
    //    bool _forward;
    //    // This means whether the cursor has reached the last EOF (with regard to this index).
    //    bool _atEOF;
    //    // This means whether or not the last move was restore.
    //    bool _lastMoveWasRestore;
    //    // This is the keystring for the saved location.
    //    std::string _saveKey;
    //    RecordId _saveLoc;
    //    // These are the same as before.
    //    std::string _prefix;
    //    std::string _identEnd;
    //    // These two store the const_iterator, which is the data structure for cursors. The one we
    //    // use depends on _forward.
    //    StringStore::const_iterator _forwardIt;
    //    StringStore::const_reverse_iterator _reverseIt;
    //    // This is the ordering for the key's values for multi-field keys.
    //    Ordering _order;
    //    // This stores whether or not the end position is inclusive for restore.
    //    bool _endPosIncl;
    //    // This stores the key for the end position.
    //    boost::optional<BSONObj> _endPosKey;
    //    // This stores whether or not the index is unique.
    //    bool _isUnique{true};
    //    // The next two are the same as above.
    //    std::string _KSForIdentStart;
    //    std::string _KSForIdentEnd;
    //    // Unpacked data from current position in the radix tree. Needed to iterate over indexes
    //    // containing duplicates
    //    IndexData _indexData;
    //    IndexData::const_iterator _indexDataIt;
    //    IndexData::const_iterator _indexDataEnd;
    //    size_t _reversePos;
    //};

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

    /*
     * This is the cursor class required by the sorted data interface.
     */
    class Cursor final : public ::mongo::SortedDataInterface::Cursor {
    public:
        // All the following public functions just implement the interface.
        Cursor(OperationContext* opCtx,
               bool isForward,
               // This is the ident.
               std::string _prefix,
               // This is a string immediately after the ident and before other idents.
               std::string _identEnd,
               StringStore* workingCopy,
               Ordering order,
               std::string prefixBSON,
               std::string KSForIdentEnd);
        virtual void setEndPosition(const BSONObj& key, bool inclusive) override;
        virtual boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) override;
        virtual boost::optional<KeyStringEntry> nextKeyString() override;
        virtual boost::optional<IndexKeyEntry> seek(const KeyString::Value& keyString,
                                                    RequestedInfo parts = kKeyAndLoc) override;
        virtual boost::optional<KeyStringEntry> seekForKeyString(
            const KeyString::Value& keyStringValue) override;
        virtual boost::optional<KeyStringEntry> seekExactForKeyString(
            const KeyString::Value& keyStringValue) override;
        virtual boost::optional<IndexKeyEntry> seekExact(const KeyString::Value& keyStringValue,
                                                         RequestedInfo) override;
        virtual void save() override;
        virtual void restore() override;
        virtual void detachFromOperationContext() override;
        virtual void reattachToOperationContext(OperationContext* opCtx) override;

    private:
        bool advanceNext();
        // This is a helper function to check if the cursor was explicitly set by the user or not.
        bool endPosSet();
        // This is a helper function to check if the cursor is valid or not.
        bool checkCursorValid();
        // Helper function to set index data iterators to reverse position
        void initReverseDataIterators();
        // This is a helper function for seek.
        boost::optional<IndexKeyEntry> seekAfterProcessing(BSONObj finalKey);
        boost::optional<KeyStringEntry> seekAfterProcessing(const KeyString::Value& keyString);
        OperationContext* _opCtx;
        // This is the "working copy" of the master "branch" in the git analogy.
        StringStore* _workingCopy;
        // These store the end positions.
        boost::optional<StringStore::const_iterator> _endPos;
        boost::optional<StringStore::const_reverse_iterator> _endPosReverse;
        // This means if the cursor is a forward or reverse cursor.
        bool _forward;
        // This means whether the cursor has reached the last EOF (with regard to this index).
        bool _atEOF;
        // This means whether or not the last move was restore.
        bool _lastMoveWasRestore;
        // This is the keystring for the saved location.
        std::string _saveKey;
        RecordId _saveLoc;
        // These are the same as before.
        std::string _prefix;
        std::string _identEnd;
        // These two store the const_iterator, which is the data structure for cursors. The one we
        // use depends on _forward.
        StringStore::const_iterator _forwardIt;
        StringStore::const_reverse_iterator _reverseIt;
        // This is the ordering for the key's values for multi-field keys.
        Ordering _order;
        // This stores whether or not the end position is inclusive for restore.
        bool _endPosIncl;
        // This stores the key for the end position.
        boost::optional<BSONObj> _endPosKey;
        // This stores whether or not the index is unique.
        bool _isUnique{true};
        // The next two are the same as above.
        std::string _KSForIdentStart;
        std::string _KSForIdentEnd;
        // Unpacked data from current position in the radix tree. Needed to iterate over indexes
        // containing duplicates
        IndexData _indexData;
        IndexData::const_iterator _indexDataIt;
        IndexData::const_iterator _indexDataEnd;
        size_t _reversePos;
    };
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

    /*
     * This is the cursor class required by the sorted data interface.
     */
    class Cursor final : public ::mongo::SortedDataInterface::Cursor {
    public:
        // All the following public functions just implement the interface.
        Cursor(OperationContext* opCtx,
               bool isForward,
               // This is the ident.
               std::string _prefix,
               // This is a string immediately after the ident and before other idents.
               std::string _identEnd,
               StringStore* workingCopy,
               Ordering order,
               std::string prefixBSON,
               std::string KSForIdentEnd);
        virtual void setEndPosition(const BSONObj& key, bool inclusive) override;
        virtual boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc) override;
        virtual boost::optional<KeyStringEntry> nextKeyString() override;
        virtual boost::optional<IndexKeyEntry> seek(const KeyString::Value& keyString,
                                                    RequestedInfo parts = kKeyAndLoc) override;
        virtual boost::optional<KeyStringEntry> seekForKeyString(
            const KeyString::Value& keyStringValue) override;
        virtual boost::optional<KeyStringEntry> seekExactForKeyString(
            const KeyString::Value& keyStringValue) override;
        virtual boost::optional<IndexKeyEntry> seekExact(const KeyString::Value& keyStringValue,
                                                         RequestedInfo) override;
        virtual void save() override;
        virtual void restore() override;
        virtual void detachFromOperationContext() override;
        virtual void reattachToOperationContext(OperationContext* opCtx) override;

    private:
        bool advanceNext();
        // This is a helper function to check if the cursor was explicitly set by the user or not.
        bool endPosSet();
        // This is a helper function to check if the cursor is valid or not.
        bool checkCursorValid();
        // This is a helper function for seek.
        boost::optional<IndexKeyEntry> seekAfterProcessing(BSONObj finalKey);
        boost::optional<KeyStringEntry> seekAfterProcessing(const KeyString::Value& keyString);
        OperationContext* _opCtx;
        // This is the "working copy" of the master "branch" in the git analogy.
        StringStore* _workingCopy;
        // These store the end positions.
        boost::optional<StringStore::const_iterator> _endPos;
        boost::optional<StringStore::const_reverse_iterator> _endPosReverse;
        // This means if the cursor is a forward or reverse cursor.
        bool _forward;
        // This means whether the cursor has reached the last EOF (with regard to this index).
        bool _atEOF;
        // This means whether or not the last move was restore.
        bool _lastMoveWasRestore;
        // This is the keystring for the saved location.
        std::string _saveKey;
        // These are the same as before.
        std::string _prefix;
        std::string _identEnd;
        // These two store the const_iterator, which is the data structure for cursors. The one we
        // use depends on _forward.
        StringStore::const_iterator _forwardIt;
        StringStore::const_reverse_iterator _reverseIt;
        // This is the ordering for the key's values for multi-field keys.
        Ordering _order;
        // This stores whether or not the end position is inclusive for restore.
        bool _endPosIncl;
        // This stores the key for the end position.
        boost::optional<BSONObj> _endPosKey;
        // This stores whether or not the index is unique.
        bool _isUnique{false};
        // The next two are the same as above.
        std::string _KSForIdentStart;
        std::string _KSForIdentEnd;
    };
};
}  // namespace biggie
}  // namespace mongo
