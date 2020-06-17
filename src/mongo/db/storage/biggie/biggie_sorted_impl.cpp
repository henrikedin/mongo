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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <cstring>
#include <memory>
#include <sstream>
#include <string>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/index_catalog_entry.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/biggie/biggie_recovery_unit.h"
#include "mongo/db/storage/biggie/biggie_sorted_impl.h"
#include "mongo/db/storage/biggie/store.h"
#include "mongo/db/storage/index_entry_comparison.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/util/bufreader.h"
#include "mongo/util/hex.h"
#include "mongo/util/shared_buffer.h"
#include "mongo/util/str.h"

namespace mongo {
namespace biggie {
namespace {

const Ordering allAscending = Ordering::make(BSONObj());

std::string createIndexDataEntry(RecordId loc, KeyString::TypeBits typebits) {
    uint64_t repr = loc.repr();
    uint64_t typebitsSize = typebits.getSize();
    std::string output(sizeof(loc) + sizeof(typebitsSize) + typebitsSize, '\0');

    std::memcpy(output.data(), &repr, sizeof(repr));
    std::memcpy(output.data() + sizeof(repr), &typebitsSize, sizeof(typebitsSize));
    std::memcpy(
        output.data() + sizeof(repr) + sizeof(typebitsSize), typebits.getBuffer(), typebitsSize);

    return output;
}

void prefixKeyString(KeyString::Builder* keyString, std::string prefixToUse) {
    BSONObjBuilder b;
    b.append("", prefixToUse);                                               // prefix
    b.append("", StringData(keyString->getBuffer(), keyString->getSize()));  // key

    keyString->resetToKey(b.obj(), allAscending);
}

void prefixKeyStringStandard(KeyString::Builder* keyString, RecordId loc, std::string prefixToUse) {
    BSONObjBuilder b;
    b.append("", prefixToUse);                                               // prefix
    b.append("", StringData(keyString->getBuffer(), keyString->getSize()));  // key

    keyString->resetToKey(b.obj(), allAscending, loc);
}

std::string createRadixKeyFromObj(const BSONObj& key, std::string prefixToUse, Ordering order) {
    KeyString::Version version = KeyString::Version::kLatestVersion;
    KeyString::Builder ks(version, BSONObj::stripFieldNames(key), order);

    prefixKeyString(&ks, prefixToUse);
    return std::string(ks.getBuffer(), ks.getSize());
}

std::string createRadixKeyFromKS(const KeyString::Value& keyString, std::string prefixToUse) {
    KeyString::Builder ks(KeyString::Version::kLatestVersion);
    ks.resetFromBuffer(
        keyString.getBuffer(),
        KeyString::sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize()));
    prefixKeyString(&ks, prefixToUse);
    return std::string(ks.getBuffer(), ks.getSize());
}

std::string createRadixKeyFromKSWithoutRecordId(const KeyString::Value& keyString,
                                                std::string prefixToUse) {
    KeyString::Builder ks(KeyString::Version::kLatestVersion);
    ks.resetFromBuffer(keyString.getBuffer(), keyString.getSize());
    prefixKeyString(&ks, prefixToUse);
    return std::string(ks.getBuffer(), ks.getSize());
}

std::string createStandardRadixKeyFromObj(const BSONObj& key,
                                          RecordId loc,
                                          std::string prefixToUse,
                                          Ordering order) {
    KeyString::Version version = KeyString::Version::kLatestVersion;
    KeyString::Builder ks(version, BSONObj::stripFieldNames(key), order);

    prefixKeyStringStandard(&ks, loc, prefixToUse);
    return std::string(ks.getBuffer(), ks.getSize());
}

std::string createStandardRadixKeyFromKS(const KeyString::Value& keyString,
                                         RecordId loc,
                                         std::string prefixToUse) {
    KeyString::Builder ks(KeyString::Version::kLatestVersion);
    ks.resetFromBuffer(
        keyString.getBuffer(),
        KeyString::sizeWithoutRecordIdAtEnd(keyString.getBuffer(), keyString.getSize()));
    prefixKeyStringStandard(&ks, loc, prefixToUse);
    return std::string(ks.getBuffer(), ks.getSize());
}

std::string createStandardRadixKeyFromKSWithoutRecordId(const KeyString::Value& keyString,
                                         RecordId loc,
                                         std::string prefixToUse) {
    KeyString::Builder ks(KeyString::Version::kLatestVersion);
    ks.resetFromBuffer(keyString.getBuffer(), keyString.getSize());
    prefixKeyStringStandard(&ks, loc, prefixToUse);
    return std::string(ks.getBuffer(), ks.getSize());
}

BSONObj createObjFromRadixKey(const std::string& radixKey,
                              const KeyString::TypeBits& typeBits,
                              const Ordering& order) {
    KeyString::Version version = KeyString::Version::kLatestVersion;
    KeyString::TypeBits tbOuter = KeyString::TypeBits(version);
    BSONObj bsonObj =
        KeyString::toBsonSafe(radixKey.data(), radixKey.size(), allAscending, tbOuter);

    SharedBuffer sb;
    auto it = BSONObjIterator(bsonObj);
    ++it;  // We want the second part
    KeyString::Builder ks(version);
    ks.resetFromBuffer((*it).valuestr(), (*it).valuestrsize());

    return KeyString::toBsonSafe(ks.getBuffer(), ks.getSize(), order, typeBits);
}

IndexKeyEntry createIndexKeyEntryFromRadixKey(const std::string& radixKey,
                                              RecordId loc,
                                              const KeyString::TypeBits& typeBits,
                                              const Ordering order) {
    return IndexKeyEntry(createObjFromRadixKey(radixKey, typeBits, order), loc);
}

IndexKeyEntry createIndexKeyEntryFromRadixKeyStandard(const std::string& radixKey,
                                              const std::string& indexDataEntry,
                                              const Ordering order) {
    IndexDataEntry data(indexDataEntry);
    return IndexKeyEntry(createObjFromRadixKey(radixKey, data.typeBits(), order), data.loc());
}

boost::optional<KeyStringEntry> createKeyStringEntryFromRadixKey(
    const std::string& radixKey,
    RecordId loc,
    const KeyString::TypeBits& typeBits,
    const Ordering& order) {
    auto key = createObjFromRadixKey(radixKey, typeBits, order);
    KeyString::Builder ksFinal(KeyString::Version::kLatestVersion, key, order);
    ksFinal.appendRecordId(loc);
    return KeyStringEntry(ksFinal.getValueCopy(), loc);
}

boost::optional<KeyStringEntry> createKeyStringEntryFromRadixKeyStandard(
    const std::string& radixKey,
    const std::string& indexDataEntry,
    const Ordering& order) {
    IndexDataEntry data(indexDataEntry);
    RecordId loc = data.loc();
    auto key = createObjFromRadixKey(radixKey, data.typeBits(), order);
    KeyString::Builder ksFinal(KeyString::Version::kLatestVersion, key, order);
    ksFinal.appendRecordId(loc);
    return KeyStringEntry(ksFinal.getValueCopy(), loc);
}
}  // namespace

IndexDataEntry::IndexDataEntry(const std::string& indexDataEntry)
    : _buffer(reinterpret_cast<const uint8_t*>(indexDataEntry.data())) {}

RecordId IndexDataEntry::loc() const {
    uint64_t repr;
    std::memcpy(&repr, _buffer, sizeof(uint64_t));
    return RecordId(repr);
}

KeyString::TypeBits IndexDataEntry::typeBits() const {
    uint64_t size;
    std::memcpy(&size, _buffer + sizeof(uint64_t), sizeof(uint64_t));

    BufReader reader(_buffer + sizeof(uint64_t)*2, size);
    return KeyString::TypeBits::fromBuffer(KeyString::Version::kLatestVersion, &reader);
}

bool IndexData::add(RecordId loc, KeyString::TypeBits typeBits) {
    return _keys.emplace(loc, std::move(typeBits)).second;
}

bool IndexData::add_hint(const_iterator hint, RecordId loc, KeyString::TypeBits typeBits) {
    auto before = _keys.size();
    _keys.emplace_hint(hint, loc, std::move(typeBits));
    return _keys.size() > before;
}

bool IndexData::remove(RecordId loc) {
    return _keys.erase(loc) > 0;
}

std::string IndexData::serialize() const {
    std::stringstream buffer;

    auto writeBytes = [&](const char* data, size_t size) { buffer.write(data, size); };
    auto writeUInt64 = [&](uint64_t val) {
        writeBytes(reinterpret_cast<const char*>(&val), sizeof(val));
    };

    writeUInt64(_keys.size());
    for (const auto& [recordId, typeBits] : _keys) {
        writeUInt64(recordId.repr());

        uint64_t typeBitsSize = typeBits.getSize();
        writeUInt64(typeBitsSize);
        writeBytes(typeBits.getBuffer(), typeBitsSize);
    }

    return buffer.str();
}
IndexData IndexData::deserialize(const std::string& serializedIndexData) {
    auto begin = serializedIndexData.begin();
    auto end = serializedIndexData.end();

    auto readBytes = [&](std::size_t num) {
        invariant((end - begin) >= static_cast<ptrdiff_t>(num));
        auto before = begin;
        begin += num;
        return &*before;
    };

    auto readUInt64 = [&]() {
        uint64_t val;
        auto ptr = readBytes(sizeof(val));
        std::memcpy(&val, ptr, sizeof(val));
        return val;
    };

    IndexData indexData;
    auto numKeys = readUInt64();
    for (uint64_t i = 0; i < numKeys; ++i) {
        auto repr = readUInt64();
        auto typeBitsSize = readUInt64();
        auto typeBitsBuffer = readBytes(typeBitsSize);

        BufReader reader(typeBitsBuffer, typeBitsSize);
        indexData._keys.emplace_hint(
            indexData._keys.end(),
            RecordId(repr),
            KeyString::TypeBits::fromBuffer(KeyString::Version::kLatestVersion, &reader));
    }
    return indexData;
}

size_t IndexData::decodeSize(const std::string& serializedIndexData) {
    invariant(serializedIndexData.size() >= sizeof(uint64_t));
    uint64_t val;
    std::memcpy(&val, serializedIndexData.data(), sizeof(uint64_t));
    return val;
}

SortedDataUniqueBuilderInterface::SortedDataUniqueBuilderInterface(
    OperationContext* opCtx,
    bool dupsAllowed,
    Ordering order,
    const std::string& prefix,
    const std::string& identEnd,
    const NamespaceString& collectionNamespace,
    const std::string& indexName,
    const BSONObj& keyPattern,
    const BSONObj& collation)
    : _opCtx(opCtx),
      _dupsAllowed(dupsAllowed),
      _order(order),
      _prefix(prefix),
      _identEnd(identEnd),
      _collectionNamespace(collectionNamespace),
      _indexName(indexName),
      _keyPattern(keyPattern),
      _collation(collation) {}

void SortedDataUniqueBuilderInterface::commit(bool mayInterrupt) {
    WriteUnitOfWork wunit(_opCtx);
    wunit.commit();
}

Status SortedDataUniqueBuilderInterface::addKey(const KeyString::Value& keyString) {
    dassert(KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize()).isValid());
    StringStore* workingCopy(RecoveryUnit::get(_opCtx)->getHead());
    RecordId loc = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());

    std::string key = createRadixKeyFromKS(keyString, _prefix);
    auto it = workingCopy->find(key);
    if (it != workingCopy->end()) {
        if (!_dupsAllowed) {
            // There was an attempt to create an index entry with a different RecordId while dups
            // were not allowed.
            auto obj = KeyString::toBson(keyString, _order);
            return buildDupKeyErrorStatus(
                obj, _collectionNamespace, _indexName, _keyPattern, _collation);
        }

        IndexData data = IndexData::deserialize(it->second);
        // Bulk builder add keys in ascending order so we should insert at the end
        if (!data.add_hint(data.end(), loc, keyString.getTypeBits())) {
            // Already indexed
            return Status::OK();
        }

        workingCopy->update({std::move(key), data.serialize()});
    } else {
        IndexData data;
        data.add(loc, keyString.getTypeBits());
        workingCopy->insert({std::move(key), data.serialize()});
    }

    RecoveryUnit::get(_opCtx)->makeDirty();
    return Status::OK();
}

SortedDataBuilderInterface* SortedDataInterfaceUnique::getBulkBuilder(OperationContext* opCtx,
                                                                      bool dupsAllowed) {
    return new SortedDataUniqueBuilderInterface(opCtx,
                                                dupsAllowed,
                                                _ordering,
                                                _prefix,
                                                _identEnd,
                                                _collectionNamespace,
                                                _indexName,
                                                _keyPattern,
                                                _collation);
}

// We append \1 to all idents we get, and therefore the KeyString with ident + \0 will only be
// before elements in this ident, and the KeyString with ident + \2 will only be after elements in
// this ident.
SortedDataInterfaceUnique::SortedDataInterfaceUnique(OperationContext* opCtx,
                                                     StringData ident,
                                                     const IndexDescriptor* desc)
    : ::mongo::SortedDataInterface(KeyString::Version::V1, Ordering::make(desc->keyPattern())),
      // All entries in this ident will have a prefix of ident + \1.
      _prefix(ident.toString().append(1, '\1')),
      // Therefore, the string ident + \2 will be greater than all elements in this ident.
      _identEnd(ident.toString().append(1, '\2')),
      _collectionNamespace(desc->getCollection()->ns()),
      _indexName(desc->indexName()),
      _keyPattern(desc->keyPattern()),
      _collation(desc->collation()),
      _isPartial(desc->isPartial()) {
    // This is the string representation of the KeyString before elements in this ident, which is
    // ident + \0. This is before all elements in this ident.
    _KSForIdentStart =
        createRadixKeyFromObj(BSONObj(), ident.toString().append(1, '\0'), _ordering);
    // Similarly, this is the string representation of the KeyString for something greater than
    // all other elements in this ident.
    _KSForIdentEnd = createRadixKeyFromObj(BSONObj(), _identEnd, _ordering);
}

SortedDataInterfaceUnique::SortedDataInterfaceUnique(const Ordering& ordering,
                                                     bool isUnique,
                                                     StringData ident)
    : ::mongo::SortedDataInterface(KeyString::Version::V1, ordering),
      _prefix(ident.toString().append(1, '\1')),
      _identEnd(ident.toString().append(1, '\2')),
      _isPartial(false) {
    _KSForIdentStart =
        createRadixKeyFromObj(BSONObj(), ident.toString().append(1, '\0'), _ordering);
    _KSForIdentEnd = createRadixKeyFromObj(BSONObj(), _identEnd, _ordering);
}

Status SortedDataInterfaceUnique::insert(OperationContext* opCtx,
                                         const KeyString::Value& keyString,
                                         bool dupsAllowed) {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    RecordId loc = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());

    std::string key = createRadixKeyFromKS(keyString, _prefix);
    auto it = workingCopy->find(key);
    if (it != workingCopy->end()) {
        if (!dupsAllowed) {
            // There was an attempt to create an index entry with a different RecordId while
            // dups were not allowed.
            auto obj = KeyString::toBson(keyString, _ordering);
            return buildDupKeyErrorStatus(
                obj, _collectionNamespace, _indexName, _keyPattern, _collation);
        }

        IndexData data = IndexData::deserialize(it->second);
        if (!data.add(loc, keyString.getTypeBits())) {
            // Already indexed
            return Status::OK();
        }

        workingCopy->update({std::move(key), data.serialize()});
    } else {
        IndexData data;
        data.add(loc, keyString.getTypeBits());
        workingCopy->insert({std::move(key), data.serialize()});
    }
    RecoveryUnit::get(opCtx)->makeDirty();
    return Status::OK();
}

void SortedDataInterfaceUnique::unindex(OperationContext* opCtx,
                                        const KeyString::Value& keyString,
                                        bool dupsAllowed) {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    RecordId loc = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());

    auto key = createRadixKeyFromKS(keyString, _prefix);
    auto it = workingCopy->find(key);
    if (it != workingCopy->end()) {
        IndexData data = IndexData::deserialize(it->second);
        if (!data.remove(loc))
            return;  // loc not found, nothing to unindex

        if (data.empty()) {
            workingCopy->erase(key);
        } else {
            workingCopy->update({std::move(key), data.serialize()});
        }
        RecoveryUnit::get(opCtx)->makeDirty();
    }
}

// This function is, as of now, not in the interface, but there exists a server ticket to add
// truncate to the list of commands able to be used.
Status SortedDataInterfaceUnique::truncate(mongo::RecoveryUnit* ru) {
    auto bRu = checked_cast<biggie::RecoveryUnit*>(ru);
    StringStore* workingCopy(bRu->getHead());
    std::vector<std::string> toDelete;
    auto end = workingCopy->upper_bound(_KSForIdentEnd);
    for (auto it = workingCopy->lower_bound(_KSForIdentStart); it != end; ++it) {
        toDelete.push_back(it->first);
    }
    if (!toDelete.empty()) {
        for (const auto& key : toDelete)
            workingCopy->erase(key);
        bRu->makeDirty();
    }

    return Status::OK();
}

Status SortedDataInterfaceUnique::dupKeyCheck(OperationContext* opCtx,
                                              const KeyString::Value& key) {
    // invariant(_isUnique);
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());

    std::string radixKey = createRadixKeyFromKSWithoutRecordId(key, _prefix);
    auto it = workingCopy->find(radixKey);
    if (it == workingCopy->end())
        return Status::OK();

    if (IndexData::decodeSize(it->second) > 1) {
        return buildDupKeyErrorStatus(
            key, _collectionNamespace, _indexName, _keyPattern, _collation, _ordering);
    }

    return Status::OK();
}

void SortedDataInterfaceUnique::fullValidate(OperationContext* opCtx,
                                             long long* numKeysOut,
                                             ValidateResults* fullResults) const {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    long long numKeys = 0;
    auto it = workingCopy->lower_bound(_KSForIdentStart);
    while (it != workingCopy->end() && it->first.compare(_KSForIdentEnd) < 0) {
        numKeys += IndexData::decodeSize(it->second);
        ++it;
    }
    *numKeysOut = numKeys;
}

bool SortedDataInterfaceUnique::appendCustomStats(OperationContext* opCtx,
                                                  BSONObjBuilder* output,
                                                  double scale) const {
    return false;
}

long long SortedDataInterfaceUnique::getSpaceUsedBytes(OperationContext* opCtx) const {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    size_t totalSize = 0;
    StringStore::const_iterator it = workingCopy->lower_bound(_KSForIdentStart);
    StringStore::const_iterator end = workingCopy->upper_bound(_KSForIdentEnd);
    int64_t numElements = workingCopy->distance(it, end);
    for (int i = 0; i < numElements; i++) {
        totalSize += it->first.length();
        ++it;
    }
    return (long long)totalSize;
}

bool SortedDataInterfaceUnique::isEmpty(OperationContext* opCtx) {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    return workingCopy->distance(workingCopy->lower_bound(_KSForIdentStart),
                                 workingCopy->upper_bound(_KSForIdentEnd)) == 0;
}

std::unique_ptr<mongo::SortedDataInterface::Cursor> SortedDataInterfaceUnique::newCursor(
    OperationContext* opCtx, bool isForward) const {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());

    return std::make_unique<SortedDataInterfaceUnique::Cursor>(opCtx,
                                                               isForward,
                                                               _prefix,
                                                               _identEnd,
                                                               workingCopy,
                                                               _ordering,
                                                               _KSForIdentStart,
                                                               _KSForIdentEnd);
}

Status SortedDataInterfaceUnique::initAsEmpty(OperationContext* opCtx) {
    return Status::OK();
}

// Cursor
SortedDataInterfaceUnique::Cursor::Cursor(OperationContext* opCtx,
                                          bool isForward,
                                          std::string _prefix,
                                          std::string _identEnd,
                                          StringStore* workingCopy,
                                          Ordering order,
                                          std::string _KSForIdentStart,
                                          std::string identEndBSON)
    : _opCtx(opCtx),
      _workingCopy(workingCopy),
      _endPos(boost::none),
      _endPosReverse(boost::none),
      _forward(isForward),
      _atEOF(false),
      _lastMoveWasRestore(false),
      _prefix(_prefix),
      _identEnd(_identEnd),
      _forwardIt(workingCopy->begin()),
      _reverseIt(workingCopy->rbegin()),
      _order(order),
      _endPosIncl(false),
      _KSForIdentStart(_KSForIdentStart),
      _KSForIdentEnd(identEndBSON) {}

bool SortedDataInterfaceUnique::Cursor::advanceNext() {
    if (!_atEOF) {
        // If the last move was restore, then we don't need to advance the cursor, since the user
        // never got the value the cursor was pointing to in the first place. However,
        // _lastMoveWasRestore will go through extra logic on a unique index, since unique indexes
        // are not allowed to return the same key twice.
        if (_lastMoveWasRestore) {
            _lastMoveWasRestore = false;
        } else {
            // Iterate over duplicates before moving to the next item in the radix tree
            if (!_indexData.empty()) {
                if (_forward) {
                    if (++_forwardIndexDataIt != _forwardIndexDataEnd)
                        return true;
                } else {
                    if (++_reverseIndexDataIt != _reverseIndexDataEnd)
                        return true;
                }
            }
            // We basically just check to make sure the cursor is in the ident.
            if (_forward && checkCursorValid()) {
                ++_forwardIt;
            } else if (!_forward && checkCursorValid()) {
                ++_reverseIt;
            }
            // We check here to make sure that we are on the correct side of the end position, and
            // that the cursor is still in the ident after advancing.
            if (!checkCursorValid()) {
                _atEOF = true;
                return false;
            }
        }
    } else {
        _lastMoveWasRestore = false;
        return false;
    }

    // We have moved to a new position in the tree, initialize index data for iterating over
    // duplicates
    if (_forward) {
        _indexData = IndexData::deserialize(_forwardIt->second);
        _forwardIndexDataIt = _indexData.begin();
        _forwardIndexDataEnd = _indexData.end();
    } else {
        _indexData = IndexData::deserialize(_reverseIt->second);
        _reverseIndexDataIt = _indexData.rbegin();
        _reverseIndexDataEnd = _indexData.rend();
    }
    return true;
}

// This function checks whether or not the cursor end position was set by the user or not.
bool SortedDataInterfaceUnique::Cursor::endPosSet() {
    return (_forward && _endPos != boost::none) || (!_forward && _endPosReverse != boost::none);
}

// This function checks whether or not a cursor is valid. In particular, it checks 1) whether the
// cursor is at end() or rend(), 2) whether the cursor is on the wrong side of the end position
// if it was set, and 3) whether the cursor is still in the ident.
bool SortedDataInterfaceUnique::Cursor::checkCursorValid() {
    if (_forward) {
        if (_forwardIt == _workingCopy->end()) {
            return false;
        }
        if (endPosSet()) {
            // The endPos must be in the ident, at most one past the ident, or end. Therefore, the
            // endPos includes the check for being inside the ident
            if (_endPosIncl && _isUnique) {
                if (*_endPos == _workingCopy->end())
                    return true;

                // For unique indexes, we need to check if the cursor moved up a position when it
                // was restored. This isn't required for non-unique indexes because we store the
                // RecordId in the KeyString and use a "<" comparison instead of "<=" since we know
                // that no RecordId will ever reach RecordId::max() so we don't need to check the
                // equal side of things. This assumption doesn't hold for unique index KeyStrings.
                std::string endPosKeyString = createRadixKeyFromObj(*_endPosKey, _prefix, _order);

                if (_forwardIt->first.compare(endPosKeyString) <= 0)
                    return true;
                return false;
            }

            return *_endPos == _workingCopy->end() ||
                _forwardIt->first.compare((*_endPos)->first) < 0;
        }
        return _forwardIt->first.compare(_KSForIdentEnd) <= 0;
    } else {
        // This is a reverse cursor
        if (_reverseIt == _workingCopy->rend()) {
            return false;
        }
        if (endPosSet()) {
            if (_endPosIncl && _isUnique) {
                if (*_endPosReverse == _workingCopy->rend())
                    return true;

                std::string endPosKeyString = createRadixKeyFromObj(*_endPosKey, _prefix, _order);

                if (_reverseIt->first.compare(endPosKeyString) >= 0)
                    return true;
                return false;
            }

            return *_endPosReverse == _workingCopy->rend() ||
                _reverseIt->first.compare((*_endPosReverse)->first) > 0;
        }
        return _reverseIt->first.compare(_KSForIdentStart) >= 0;
    }
}

void SortedDataInterfaceUnique::Cursor::setEndPosition(const BSONObj& key, bool inclusive) {
    StringStore* workingCopy(RecoveryUnit::get(_opCtx)->getHead());
    if (key.isEmpty()) {
        _endPos = boost::none;
        _endPosReverse = boost::none;
        return;
    }
    _endPosIncl = inclusive;
    _endPosKey = key;
    std::string radixKey = createRadixKeyFromObj(key, _prefix, _order);
    // If forward and inclusive or reverse and not inclusive, then we use the last element in this
    // ident. Otherwise, we use the first as our bound.
    StringStore::const_iterator it;
    if ((_forward && inclusive) || (!_forward && !inclusive))
        it = workingCopy->upper_bound(radixKey);
    else
        it = workingCopy->lower_bound(radixKey);
    if (_forward)
        _endPos = it;
    else
        _endPosReverse = StringStore::const_reverse_iterator(it);
}

boost::optional<IndexKeyEntry> SortedDataInterfaceUnique::Cursor::next(RequestedInfo parts) {
    if (!advanceNext()) {
        return {};
    }

    if (_forward) {
        return createIndexKeyEntryFromRadixKey(
            _forwardIt->first, _forwardIndexDataIt->first, _forwardIndexDataIt->second, _order);
    }
    return createIndexKeyEntryFromRadixKey(
        _reverseIt->first, _reverseIndexDataIt->first, _reverseIndexDataIt->second, _order);
}

boost::optional<KeyStringEntry> SortedDataInterfaceUnique::Cursor::nextKeyString() {
    if (!advanceNext()) {
        return {};
    }

    if (_forward) {
        return createKeyStringEntryFromRadixKey(
            _forwardIt->first, _forwardIndexDataIt->first, _forwardIndexDataIt->second, _order);
    }
    return createKeyStringEntryFromRadixKey(
        _reverseIt->first, _reverseIndexDataIt->first, _reverseIndexDataIt->second, _order);
}

boost::optional<IndexKeyEntry> SortedDataInterfaceUnique::Cursor::seekAfterProcessing(
    BSONObj finalKey) {
    std::string workingCopyBound;

    KeyString::Builder ks(KeyString::Version::kLatestVersion, finalKey, _order);
    auto ksEntry = seekAfterProcessing(ks.getValueCopy());

    const BSONObj bson = KeyString::toBson(ksEntry->keyString.getBuffer(),
                                           ksEntry->keyString.getSize(),
                                           _order,
                                           ksEntry->keyString.getTypeBits());
    return IndexKeyEntry(bson, ksEntry->loc);
}

boost::optional<KeyStringEntry> SortedDataInterfaceUnique::Cursor::seekAfterProcessing(
    const KeyString::Value& keyStringVal) {

    KeyString::Discriminator discriminator = KeyString::decodeDiscriminator(
        keyStringVal.getBuffer(), keyStringVal.getSize(), _order, keyStringVal.getTypeBits());

    bool inclusive;
    switch (discriminator) {
        case KeyString::Discriminator::kInclusive:
            inclusive = true;
            break;
        case KeyString::Discriminator::kExclusiveBefore:
            inclusive = _forward;
            break;
        case KeyString::Discriminator::kExclusiveAfter:
            inclusive = !_forward;
            break;
    }

    // If the key is empty and it's not inclusive, then no elements satisfy this seek.
    if (keyStringVal.isEmpty() && !inclusive) {
        _atEOF = true;
        return boost::none;
    }
    std::string radixKey = createRadixKeyFromKSWithoutRecordId(keyStringVal, _prefix);

    StringStore::const_iterator it;
    // Forward inclusive seek uses lower_bound and exclusive upper_bound. For reverse iterators this
    // is also reversed.
    if ((_forward && inclusive) || (!_forward && !inclusive))
        it = _workingCopy->lower_bound(radixKey);
    else
        it = _workingCopy->upper_bound(radixKey);
    if (_forward)
        _forwardIt = it;
    else
        _reverseIt = StringStore::const_reverse_iterator(it);

    // Here, we check to make sure the iterator doesn't fall off the data structure and is
    // in the ident. We also check to make sure it is on the correct side of the end
    // position, if it was set.
    if (!checkCursorValid()) {
        _atEOF = true;
        return boost::none;
    }

    // We have seeked to an entry in the tree. Now unpack the data and initialize iterators to point
    // to the first entry if this index contains duplicates
    if (_forward) {
        _indexData = IndexData::deserialize(_forwardIt->second);
        _forwardIndexDataIt = _indexData.begin();
        _forwardIndexDataEnd = _indexData.end();
        return createKeyStringEntryFromRadixKey(
            _forwardIt->first, _forwardIndexDataIt->first, _forwardIndexDataIt->second, _order);
    } else {
        _indexData = IndexData::deserialize(_reverseIt->second);
        _reverseIndexDataIt = _indexData.rbegin();
        _reverseIndexDataEnd = _indexData.rend();
        return createKeyStringEntryFromRadixKey(
            _reverseIt->first, _reverseIndexDataIt->first, _reverseIndexDataIt->second, _order);
    }
}

boost::optional<IndexKeyEntry> SortedDataInterfaceUnique::Cursor::seek(
    const KeyString::Value& keyString, RequestedInfo parts) {
    boost::optional<KeyStringEntry> ksValue = seekForKeyString(keyString);
    if (ksValue) {
        BSONObj bson = KeyString::toBson(ksValue->keyString.getBuffer(),
                                         ksValue->keyString.getSize(),
                                         _order,
                                         ksValue->keyString.getTypeBits());
        return IndexKeyEntry(bson, ksValue->loc);
    }
    return boost::none;
}

boost::optional<KeyStringEntry> SortedDataInterfaceUnique::Cursor::seekForKeyString(
    const KeyString::Value& keyStringValue) {
    _lastMoveWasRestore = false;
    _atEOF = false;
    return seekAfterProcessing(keyStringValue);
}

boost::optional<KeyStringEntry> SortedDataInterfaceUnique::Cursor::seekExactForKeyString(
    const KeyString::Value& keyStringValue) {
    dassert(KeyString::decodeDiscriminator(keyStringValue.getBuffer(),
                                           keyStringValue.getSize(),
                                           _order,
                                           keyStringValue.getTypeBits()) ==
            KeyString::Discriminator::kInclusive);
    auto ksEntry = seekForKeyString(keyStringValue);
    if (!ksEntry) {
        return {};
    }
    if (KeyString::compare(ksEntry->keyString.getBuffer(),
                           keyStringValue.getBuffer(),
                           KeyString::sizeWithoutRecordIdAtEnd(ksEntry->keyString.getBuffer(),
                                                               ksEntry->keyString.getSize()),
                           keyStringValue.getSize()) == 0) {
        return KeyStringEntry(ksEntry->keyString, ksEntry->loc);
    }
    return {};
}

boost::optional<IndexKeyEntry> SortedDataInterfaceUnique::Cursor::seekExact(
    const KeyString::Value& keyStringValue, RequestedInfo parts) {
    auto ksEntry = seekExactForKeyString(keyStringValue);
    if (!ksEntry) {
        return {};
    }

    BSONObj bson;
    if (parts & SortedDataInterface::Cursor::kWantKey) {
        bson = KeyString::toBson(ksEntry->keyString.getBuffer(),
                                 ksEntry->keyString.getSize(),
                                 _order,
                                 ksEntry->keyString.getTypeBits());
    }
    return IndexKeyEntry(std::move(bson), ksEntry->loc);
}

void SortedDataInterfaceUnique::Cursor::save() {
    _atEOF = false;
    if (_lastMoveWasRestore) {
        return;
    } else if (_forward && _forwardIt != _workingCopy->end()) {
        _saveKey = _forwardIt->first;
        if (!_indexData.empty()) {
            _saveLoc = _forwardIndexDataIt->first;
        }
    } else if (!_forward && _reverseIt != _workingCopy->rend()) {  // reverse
        _saveKey = _reverseIt->first;
        if (!_indexData.empty()) {
            _saveLoc = _reverseIndexDataIt->first;
        }
    } else {
        _saveKey = "";
        _saveLoc = RecordId();
    }
}

void SortedDataInterfaceUnique::Cursor::restore() {
    StringStore* workingCopy(RecoveryUnit::get(_opCtx)->getHead());

    this->_workingCopy = workingCopy;

    // Here, we have to reset the end position if one was set earlier.
    if (endPosSet()) {
        setEndPosition(*_endPosKey, _endPosIncl);
    }

    // We reset the cursor, and make sure it's within the end position bounds. It doesn't matter if
    // the cursor is not in the ident right now, since that will be taken care of upon the call to
    // next().
    if (_forward) {
        if (_saveKey.length() == 0) {
            _forwardIt = workingCopy->end();
        } else {
            _forwardIt = workingCopy->lower_bound(_saveKey);
        }
        _lastMoveWasRestore = true;
        if (_saveLoc != RecordId() && _forwardIt != workingCopy->end() &&
            _forwardIt->first == _saveKey) {
            _indexData = IndexData::deserialize(_forwardIt->second);
            _forwardIndexDataIt = _indexData.lower_bound(_saveLoc);
            _forwardIndexDataEnd = _indexData.end();
            if (_forwardIndexDataIt == _forwardIndexDataEnd) {
                // We reached the end of the index data, so we need to go to the next item in the
                // radix tree to be positioned on a valid item
                ++_forwardIt;
                if (_forwardIt != workingCopy->end()) {
                    _indexData = IndexData::deserialize(_forwardIt->second);
                    _forwardIndexDataIt = _indexData.begin();
                    _forwardIndexDataEnd = _indexData.end();
                }
            } else {
                // If we restore to the exact item that we saved then don't flag that we restored so
                // we will advance to the next item instead of returning the same twice.
                // Unique indexes disregard difference in location and forces the cursor to advance
                // to guarantee that we never return the same key twice
                _lastMoveWasRestore = !_isUnique && _forwardIndexDataIt->first != _saveLoc;
            }
        }
        if (!checkCursorValid()) {
            _atEOF = true;
            return;
        }
    } else {
        // Now we are dealing with reverse cursors, and use similar logic.
        if (_saveKey.length() == 0) {
            _reverseIt = workingCopy->rend();
        } else {
            _reverseIt = StringStore::const_reverse_iterator(workingCopy->upper_bound(_saveKey));
        }
        _lastMoveWasRestore = true;
        if (_saveLoc != RecordId() && _reverseIt != workingCopy->rend() &&
            _reverseIt->first == _saveKey) {
            _indexData = IndexData::deserialize(_reverseIt->second);
            _reverseIndexDataIt =
                IndexData::const_reverse_iterator(_indexData.upper_bound(_saveLoc));
            _reverseIndexDataEnd = _indexData.rend();
            if (_reverseIndexDataIt == _reverseIndexDataEnd) {
                ++_reverseIt;
                if (_reverseIt != workingCopy->rend()) {
                    _indexData = IndexData::deserialize(_reverseIt->second);
                    _reverseIndexDataIt = _indexData.rbegin();
                    _reverseIndexDataEnd = _indexData.rend();
                }
            } else {
                _lastMoveWasRestore = !_isUnique && _reverseIndexDataIt->first != _saveLoc;
            }
        }
        if (!checkCursorValid()) {
            _atEOF = true;
            return;
        }
    }
}

void SortedDataInterfaceUnique::Cursor::detachFromOperationContext() {
    _opCtx = nullptr;
}

void SortedDataInterfaceUnique::Cursor::reattachToOperationContext(OperationContext* opCtx) {
    this->_opCtx = opCtx;
}

SortedDataStandardBuilderInterface::SortedDataStandardBuilderInterface(
    OperationContext* opCtx,
    bool dupsAllowed,
    Ordering order,
    const std::string& prefix,
    const std::string& identEnd,
    const NamespaceString& collectionNamespace,
    const std::string& indexName,
    const BSONObj& keyPattern,
    const BSONObj& collation)
    : _opCtx(opCtx),
      _dupsAllowed(dupsAllowed),
      _order(order),
      _prefix(prefix),
      _identEnd(identEnd),
      _collectionNamespace(collectionNamespace),
      _indexName(indexName),
      _keyPattern(keyPattern),
      _collation(collation) {}

void SortedDataStandardBuilderInterface::commit(bool mayInterrupt) {
    WriteUnitOfWork wunit(_opCtx);
    wunit.commit();
}

Status SortedDataStandardBuilderInterface::addKey(const KeyString::Value& keyString) {
    dassert(KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize()).isValid());
    StringStore* workingCopy(RecoveryUnit::get(_opCtx)->getHead());
    RecordId loc = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());

    std::string key = createStandardRadixKeyFromKS(keyString, loc, _prefix);
    bool inserted =
        workingCopy->insert({std::move(key), createIndexDataEntry(loc, keyString.getTypeBits())})
            .second;
    if (inserted)
        RecoveryUnit::get(_opCtx)->makeDirty();
    return Status::OK();
}

SortedDataBuilderInterface* SortedDataInterfaceStandard::getBulkBuilder(OperationContext* opCtx,
                                                                        bool dupsAllowed) {
    return new SortedDataStandardBuilderInterface(opCtx,
                                                  dupsAllowed,
                                                  _ordering,
                                                  _prefix,
                                                  _identEnd,
                                                  _collectionNamespace,
                                                  _indexName,
                                                  _keyPattern,
                                                  _collation);
}

// We append \1 to all idents we get, and therefore the KeyString with ident + \0 will only be
// before elements in this ident, and the KeyString with ident + \2 will only be after elements in
// this ident.
SortedDataInterfaceStandard::SortedDataInterfaceStandard(OperationContext* opCtx,
                                                         StringData ident,
                                                         const IndexDescriptor* desc)
    : ::mongo::SortedDataInterface(KeyString::Version::V1, Ordering::make(desc->keyPattern())),
      // All entries in this ident will have a prefix of ident + \1.
      _prefix(ident.toString().append(1, '\1')),
      // Therefore, the string ident + \2 will be greater than all elements in this ident.
      _identEnd(ident.toString().append(1, '\2')),
      _collectionNamespace(desc->getCollection()->ns()),
      _indexName(desc->indexName()),
      _keyPattern(desc->keyPattern()),
      _collation(desc->collation()),
      _isPartial(desc->isPartial()) {
    // This is the string representation of the KeyString before elements in this ident, which is
    // ident + \0. This is before all elements in this ident.
    _KSForIdentStart = createStandardRadixKeyFromObj(
        BSONObj(), RecordId::min(), ident.toString().append(1, '\0'), _ordering);
    // Similarly, this is the string representation of the KeyString for something greater than
    // all other elements in this ident.
    _KSForIdentEnd =
        createStandardRadixKeyFromObj(BSONObj(), RecordId::min(), _identEnd, _ordering);
}

SortedDataInterfaceStandard::SortedDataInterfaceStandard(const Ordering& ordering,
                                                         bool isUnique,
                                                         StringData ident)
    : ::mongo::SortedDataInterface(KeyString::Version::V1, ordering),
      _prefix(ident.toString().append(1, '\1')),
      _identEnd(ident.toString().append(1, '\2')),
      _isPartial(false) {
    _KSForIdentStart = createStandardRadixKeyFromObj(
        BSONObj(), RecordId::min(), ident.toString().append(1, '\0'), _ordering);
    _KSForIdentEnd =
        createStandardRadixKeyFromObj(BSONObj(), RecordId::min(), _identEnd, _ordering);
}

Status SortedDataInterfaceStandard::insert(OperationContext* opCtx,
                                           const KeyString::Value& keyString,
                                           bool dupsAllowed) {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    RecordId loc = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());

    std::string key = createStandardRadixKeyFromKS(keyString, loc, _prefix);
    bool inserted =
        workingCopy->insert({std::move(key), createIndexDataEntry(loc, keyString.getTypeBits())})
            .second;
    if (inserted)
        RecoveryUnit::get(opCtx)->makeDirty();
    return Status::OK();
}

void SortedDataInterfaceStandard::unindex(OperationContext* opCtx,
                                          const KeyString::Value& keyString,
                                          bool dupsAllowed) {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    RecordId loc = KeyString::decodeRecordIdAtEnd(keyString.getBuffer(), keyString.getSize());

    auto key = createStandardRadixKeyFromKS(keyString, loc, _prefix);
    if (workingCopy->erase(key))
        RecoveryUnit::get(opCtx)->makeDirty();
}

// This function is, as of now, not in the interface, but there exists a server ticket to add
// truncate to the list of commands able to be used.
Status SortedDataInterfaceStandard::truncate(mongo::RecoveryUnit* ru) {
    auto bRu = checked_cast<biggie::RecoveryUnit*>(ru);
    StringStore* workingCopy(bRu->getHead());
    std::vector<std::string> toDelete;
    auto end = workingCopy->upper_bound(_KSForIdentEnd);
    for (auto it = workingCopy->lower_bound(_KSForIdentStart); it != end; ++it) {
        toDelete.push_back(it->first);
    }
    if (!toDelete.empty()) {
        for (const auto& key : toDelete)
            workingCopy->erase(key);
        bRu->makeDirty();
    }

    return Status::OK();
}

Status SortedDataInterfaceStandard::dupKeyCheck(OperationContext* opCtx,
                                                const KeyString::Value& key) {
    invariant(false);
    return Status::OK();
}

void SortedDataInterfaceStandard::fullValidate(OperationContext* opCtx,
                                               long long* numKeysOut,
                                               ValidateResults* fullResults) const {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    long long numKeys = 0;
    auto it = workingCopy->lower_bound(_KSForIdentStart);
    while (it != workingCopy->end() && it->first.compare(_KSForIdentEnd) < 0) {
        ++numKeys;
        ++it;
    }
    *numKeysOut = numKeys;
}

bool SortedDataInterfaceStandard::appendCustomStats(OperationContext* opCtx,
                                                    BSONObjBuilder* output,
                                                    double scale) const {
    return false;
}

long long SortedDataInterfaceStandard::getSpaceUsedBytes(OperationContext* opCtx) const {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    size_t totalSize = 0;
    StringStore::const_iterator it = workingCopy->lower_bound(_KSForIdentStart);
    StringStore::const_iterator end = workingCopy->upper_bound(_KSForIdentEnd);
    int64_t numElements = workingCopy->distance(it, end);
    for (int i = 0; i < numElements; i++) {
        totalSize += it->first.length();
        ++it;
    }
    return (long long)totalSize;
}

bool SortedDataInterfaceStandard::isEmpty(OperationContext* opCtx) {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());
    return workingCopy->distance(workingCopy->lower_bound(_KSForIdentStart),
                                 workingCopy->upper_bound(_KSForIdentEnd)) == 0;
}

std::unique_ptr<mongo::SortedDataInterface::Cursor> SortedDataInterfaceStandard::newCursor(
    OperationContext* opCtx, bool isForward) const {
    StringStore* workingCopy(RecoveryUnit::get(opCtx)->getHead());

    return std::make_unique<SortedDataInterfaceStandard::Cursor>(opCtx,
                                                                 isForward,
                                                                 _prefix,
                                                                 _identEnd,
                                                                 workingCopy,
                                                                 _ordering,
                                                                 _KSForIdentStart,
                                                                 _KSForIdentEnd);
}

Status SortedDataInterfaceStandard::initAsEmpty(OperationContext* opCtx) {
    return Status::OK();
}

// Cursor
SortedDataInterfaceStandard::Cursor::Cursor(OperationContext* opCtx,
                                            bool isForward,
                                            std::string _prefix,
                                            std::string _identEnd,
                                            StringStore* workingCopy,
                                            Ordering order,
                                            std::string _KSForIdentStart,
                                            std::string identEndBSON)
    : _opCtx(opCtx),
      _workingCopy(workingCopy),
      _endPos(boost::none),
      _endPosReverse(boost::none),
      _forward(isForward),
      _atEOF(false),
      _lastMoveWasRestore(false),
      _prefix(_prefix),
      _identEnd(_identEnd),
      _forwardIt(workingCopy->begin()),
      _reverseIt(workingCopy->rbegin()),
      _order(order),
      _endPosIncl(false),
      _KSForIdentStart(_KSForIdentStart),
      _KSForIdentEnd(identEndBSON) {}

bool SortedDataInterfaceStandard::Cursor::advanceNext() {
    if (!_atEOF) {
        // If the last move was restore, then we don't need to advance the cursor, since the user
        // never got the value the cursor was pointing to in the first place. However,
        // _lastMoveWasRestore will go through extra logic on a unique index, since unique indexes
        // are not allowed to return the same key twice.
        if (_lastMoveWasRestore) {
            _lastMoveWasRestore = false;
        } else {
            // We basically just check to make sure the cursor is in the ident.
            if (_forward && checkCursorValid()) {
                ++_forwardIt;
            } else if (!_forward && checkCursorValid()) {
                ++_reverseIt;
            }
            // We check here to make sure that we are on the correct side of the end position, and
            // that the cursor is still in the ident after advancing.
            if (!checkCursorValid()) {
                _atEOF = true;
                return false;
            }
        }
    } else {
        _lastMoveWasRestore = false;
        return false;
    }

    return true;
}

// This function checks whether or not the cursor end position was set by the user or not.
bool SortedDataInterfaceStandard::Cursor::endPosSet() {
    return (_forward && _endPos != boost::none) || (!_forward && _endPosReverse != boost::none);
}

// This function checks whether or not a cursor is valid. In particular, it checks 1) whether the
// cursor is at end() or rend(), 2) whether the cursor is on the wrong side of the end position
// if it was set, and 3) whether the cursor is still in the ident.
bool SortedDataInterfaceStandard::Cursor::checkCursorValid() {
    if (_forward) {
        if (_forwardIt == _workingCopy->end()) {
            return false;
        }
        if (endPosSet()) {
            // The endPos must be in the ident, at most one past the ident, or end. Therefore, the
            // endPos includes the check for being inside the ident
            // if (_endPosIncl && _isUnique) {
            //    if (*_endPos == _workingCopy->end())
            //        return true;

            //    // For unique indexes, we need to check if the cursor moved up a position when it
            //    // was restored. This isn't required for non-unique indexes because we store the
            //    // RecordId in the KeyString and use a "<" comparison instead of "<=" since we
            //    know
            //    // that no RecordId will ever reach RecordId::max() so we don't need to check the
            //    // equal side of things. This assumption doesn't hold for unique index KeyStrings.
            //    std::string endPosKeyString = createRadixKeyFromObj(*_endPosKey, _prefix, _order);

            //    if (_forwardIt->first.compare(endPosKeyString) <= 0)
            //        return true;
            //    return false;
            //}

            return *_endPos == _workingCopy->end() ||
                _forwardIt->first.compare((*_endPos)->first) < 0;
        }
        return _forwardIt->first.compare(_KSForIdentEnd) <= 0;
    } else {
        // This is a reverse cursor
        if (_reverseIt == _workingCopy->rend()) {
            return false;
        }
        if (endPosSet()) {
            /*if (_endPosIncl && _isUnique) {
                if (*_endPosReverse == _workingCopy->rend())
                    return true;

                std::string endPosKeyString = createRadixKeyFromObj(*_endPosKey, _prefix, _order);

                if (_reverseIt->first.compare(endPosKeyString) >= 0)
                    return true;
                return false;
            }*/

            return *_endPosReverse == _workingCopy->rend() ||
                _reverseIt->first.compare((*_endPosReverse)->first) > 0;
        }
        return _reverseIt->first.compare(_KSForIdentStart) >= 0;
    }
}

void SortedDataInterfaceStandard::Cursor::setEndPosition(const BSONObj& key, bool inclusive) {
    StringStore* workingCopy(RecoveryUnit::get(_opCtx)->getHead());
    if (key.isEmpty()) {
        _endPos = boost::none;
        _endPosReverse = boost::none;
        return;
    }
    _endPosIncl = inclusive;
    _endPosKey = key;
    // If forward and inclusive or reverse and not inclusive, then we use the last element in this
    // ident. Otherwise, we use the first as our bound.
    StringStore::const_iterator it;
    if (_forward == inclusive)
        it = workingCopy->upper_bound(
            createStandardRadixKeyFromObj(key, RecordId::max(), _prefix, _order));
    else
        it = workingCopy->lower_bound(
            createStandardRadixKeyFromObj(key, RecordId::min(), _prefix, _order));
    if (_forward)
        _endPos = it;
    else
        _endPosReverse = StringStore::const_reverse_iterator(it);
}

boost::optional<IndexKeyEntry> SortedDataInterfaceStandard::Cursor::next(RequestedInfo parts) {
    if (!advanceNext()) {
        return {};
    }

    if (_forward) {
        return createIndexKeyEntryFromRadixKeyStandard(
            _forwardIt->first, _forwardIt->second, _order);
    }
    return createIndexKeyEntryFromRadixKeyStandard(
        _reverseIt->first, _reverseIt->second, _order);
}

boost::optional<KeyStringEntry> SortedDataInterfaceStandard::Cursor::nextKeyString() {
    if (!advanceNext()) {
        return {};
    }

    if (_forward) {
        return createKeyStringEntryFromRadixKeyStandard(
            _forwardIt->first, _forwardIt->second, _order);
    }
    return createKeyStringEntryFromRadixKeyStandard(
        _reverseIt->first, _reverseIt->second, _order);
}

boost::optional<IndexKeyEntry> SortedDataInterfaceStandard::Cursor::seekAfterProcessing(
    BSONObj finalKey) {
    std::string workingCopyBound;

    KeyString::Builder ks(KeyString::Version::kLatestVersion, finalKey, _order);
    auto ksEntry = seekAfterProcessing(ks.getValueCopy());

    const BSONObj bson = KeyString::toBson(ksEntry->keyString.getBuffer(),
                                           ksEntry->keyString.getSize(),
                                           _order,
                                           ksEntry->keyString.getTypeBits());
    return IndexKeyEntry(bson, ksEntry->loc);
}

boost::optional<KeyStringEntry> SortedDataInterfaceStandard::Cursor::seekAfterProcessing(
    const KeyString::Value& keyStringVal) {

    KeyString::Discriminator discriminator = KeyString::decodeDiscriminator(
        keyStringVal.getBuffer(), keyStringVal.getSize(), _order, keyStringVal.getTypeBits());

    bool inclusive;
    switch (discriminator) {
        case KeyString::Discriminator::kInclusive:
            inclusive = true;
            break;
        case KeyString::Discriminator::kExclusiveBefore:
            inclusive = _forward;
            break;
        case KeyString::Discriminator::kExclusiveAfter:
            inclusive = !_forward;
            break;
    }

    // If the key is empty and it's not inclusive, then no elements satisfy this seek.
    if (keyStringVal.isEmpty() && !inclusive) {
        _atEOF = true;
        return boost::none;
    }

    StringStore::const_iterator it;
    // Forward inclusive seek uses lower_bound and exclusive upper_bound. For reverse iterators this
    // is also reversed.
    if (_forward == inclusive)
        it = _workingCopy->lower_bound(createStandardRadixKeyFromKSWithoutRecordId(keyStringVal, RecordId::min(), _prefix));
    else
        it = _workingCopy->upper_bound(createStandardRadixKeyFromKSWithoutRecordId(keyStringVal, RecordId::max(), _prefix));
    if (_forward)
        _forwardIt = it;
    else
        _reverseIt = StringStore::const_reverse_iterator(it);

    // Here, we check to make sure the iterator doesn't fall off the data structure and is
    // in the ident. We also check to make sure it is on the correct side of the end
    // position, if it was set.
    if (!checkCursorValid()) {
        _atEOF = true;
        return boost::none;
    }

    // We have seeked to an entry in the tree. Now unpack the data and initialize iterators to point
    // to the first entry if this index contains duplicates
    if (_forward) {
        return createKeyStringEntryFromRadixKeyStandard(
            _forwardIt->first, _forwardIt->second, _order);
    } else {
        return createKeyStringEntryFromRadixKeyStandard(
            _reverseIt->first, _reverseIt->second, _order);
    }
}

boost::optional<IndexKeyEntry> SortedDataInterfaceStandard::Cursor::seek(
    const KeyString::Value& keyString, RequestedInfo parts) {
    boost::optional<KeyStringEntry> ksValue = seekForKeyString(keyString);
    if (ksValue) {
        BSONObj bson = KeyString::toBson(ksValue->keyString.getBuffer(),
                                         ksValue->keyString.getSize(),
                                         _order,
                                         ksValue->keyString.getTypeBits());
        return IndexKeyEntry(bson, ksValue->loc);
    }
    return boost::none;
}

boost::optional<KeyStringEntry> SortedDataInterfaceStandard::Cursor::seekForKeyString(
    const KeyString::Value& keyStringValue) {
    _lastMoveWasRestore = false;
    _atEOF = false;
    return seekAfterProcessing(keyStringValue);
}

boost::optional<KeyStringEntry> SortedDataInterfaceStandard::Cursor::seekExactForKeyString(
    const KeyString::Value& keyStringValue) {
    dassert(KeyString::decodeDiscriminator(keyStringValue.getBuffer(),
                                           keyStringValue.getSize(),
                                           _order,
                                           keyStringValue.getTypeBits()) ==
            KeyString::Discriminator::kInclusive);
    auto ksEntry = seekForKeyString(keyStringValue);
    if (!ksEntry) {
        return {};
    }
    if (KeyString::compare(ksEntry->keyString.getBuffer(),
                           keyStringValue.getBuffer(),
                           KeyString::sizeWithoutRecordIdAtEnd(ksEntry->keyString.getBuffer(),
                                                               ksEntry->keyString.getSize()),
                           keyStringValue.getSize()) == 0) {
        return KeyStringEntry(ksEntry->keyString, ksEntry->loc);
    }
    return {};
}

boost::optional<IndexKeyEntry> SortedDataInterfaceStandard::Cursor::seekExact(
    const KeyString::Value& keyStringValue, RequestedInfo parts) {
    auto ksEntry = seekExactForKeyString(keyStringValue);
    if (!ksEntry) {
        return {};
    }

    BSONObj bson;
    if (parts & SortedDataInterface::Cursor::kWantKey) {
        bson = KeyString::toBson(ksEntry->keyString.getBuffer(),
                                 ksEntry->keyString.getSize(),
                                 _order,
                                 ksEntry->keyString.getTypeBits());
    }
    return IndexKeyEntry(std::move(bson), ksEntry->loc);
}

void SortedDataInterfaceStandard::Cursor::save() {
    _atEOF = false;
    if (_lastMoveWasRestore) {
        return;
    } else if (_forward && _forwardIt != _workingCopy->end()) {
        _saveKey = _forwardIt->first;
    } else if (!_forward && _reverseIt != _workingCopy->rend()) {  // reverse
        _saveKey = _reverseIt->first;
    } else {
        _saveKey = "";
    }
}

void SortedDataInterfaceStandard::Cursor::restore() {
    StringStore* workingCopy(RecoveryUnit::get(_opCtx)->getHead());

    this->_workingCopy = workingCopy;

    // Here, we have to reset the end position if one was set earlier.
    if (endPosSet()) {
        setEndPosition(*_endPosKey, _endPosIncl);
    }

    // We reset the cursor, and make sure it's within the end position bounds. It doesn't matter if
    // the cursor is not in the ident right now, since that will be taken care of upon the call to
    // next().
    if (_forward) {
        if (_saveKey.length() == 0) {
            _forwardIt = workingCopy->end();
        } else {
            _forwardIt = workingCopy->lower_bound(_saveKey);
        }
        if (!checkCursorValid()) {
            _atEOF = true;
            _lastMoveWasRestore = true;
            return;
        }
        _lastMoveWasRestore = (_forwardIt->first.compare(_saveKey) != 0);
    } else {
        // Now we are dealing with reverse cursors, and use similar logic.
        if (_saveKey.length() == 0) {
            _reverseIt = workingCopy->rend();
        } else {
            _reverseIt = StringStore::const_reverse_iterator(workingCopy->upper_bound(_saveKey));
        }
        if (!checkCursorValid()) {
            _atEOF = true;
            _lastMoveWasRestore = true;
            return;
        }
        _lastMoveWasRestore = (_reverseIt->first.compare(_saveKey) != 0);
    }
}

void SortedDataInterfaceStandard::Cursor::detachFromOperationContext() {
    _opCtx = nullptr;
}

void SortedDataInterfaceStandard::Cursor::reattachToOperationContext(OperationContext* opCtx) {
    this->_opCtx = opCtx;
}

}  // namespace biggie
}  // namespace mongo
