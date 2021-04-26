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

#pragma once

#include "mongo/base/string_data.h"
#include "mongo/bson/bsonelement.h"

#include <memory>
#include <string>
#include <vector>

namespace mongo {

class MinMaxValue
{
public:
    std::unique_ptr<char[]> value;
    int size = 0;
};

class MinMaxData {
public:
    uint8_t type() const {
        return _type;
    }

    /**
    * Flag to indicate if this MinMax::Data was updated since last clear.
    */
    bool updated() const {
        return _updated;
    }

    /**
        * Clear update flag.
        */
    void clearUpdated() {
        _updated = false;
    }

    BSONElement value() const {
        return BSONElement(_value.value.get(), 1, _value.size, BSONElement::CachedSizeTag{});
    }
    BSONType valueType() const {
        return (BSONType)_value.value[0];
    }
    
    int valueSize() const {
        return _value.size;
    }

    void setObject() {
        _type = 1; // kObject
    }

    void setArray() {
        _type = 2; // kArray
    }

    void setValue(const BSONElement& elem) {
        auto requiredSize = elem.size() - elem.fieldNameSize() + 1;
        if (_value.size < requiredSize) {
            _value.value = std::make_unique<char[]>(requiredSize);
        }
        // Store element as BSONElement buffer but strip out the field name
        _value.value[0] = elem.type();
        _value.value[1] = '\0';
        memcpy(_value.value.get() + 2, elem.value(), elem.valuesize());
        _value.size = requiredSize;
        _type = 3; // kValue
        _updated = true;
    }

private:
    uint8_t _type = 0;
    bool _updated = false;
    MinMaxValue _value;
};

class MinMaxObj;

class MinMaxElement {
    friend class MinMaxObj;
public:
    StringData fieldName() const {
        return _fieldName;
    }

    MinMaxData& min() {
        return _min;
    }

    const MinMaxData& min() const {
        return _min;
    }

    MinMaxData& max() {
        return _min;
    }

    const MinMaxData& max() const {
        return _min;
    }

private:
    std::string _fieldName;
    MinMaxData _min;
    MinMaxData _max;
};

struct MinMaxEntry {
public:
    uint32_t _offsetEnd;
    uint32_t _offsetParent;
    MinMaxElement _element;
};


class EntryIterator {
    friend class MinMaxObj;

public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = ptrdiff_t;
    using value_type = MinMaxElement;
    using pointer = MinMaxElement*;
    using reference = MinMaxElement&;

    EntryIterator(std::vector<MinMaxEntry>::iterator pos)
        : _pos(pos) {}

    pointer operator->() {
        return &_pos->_element;
    }
    reference operator*() {
        return _pos->_element;
    }

    EntryIterator& operator++() {
        _pos += _pos->_offsetEnd;
        return *this;
    }

    bool operator==(const EntryIterator& rhs) const {
        return _pos == rhs._pos;
    }

    bool operator!=(const EntryIterator& rhs) const {
        return !operator==(rhs);
    }

private:
    std::vector<MinMaxEntry>::iterator _pos;
};

class ConstEntryIterator {
    friend class MinMaxObj;

public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = ptrdiff_t;
    using value_type = const MinMaxElement;
    using pointer = const MinMaxElement*;
    using reference = const MinMaxElement&;

    ConstEntryIterator(std::vector<MinMaxEntry>::const_iterator pos)
        : _pos(pos) {}

    pointer operator->() {
        return &_pos->_element;
    }
    reference operator*() {
        return _pos->_element;
    }

    ConstEntryIterator& operator++() {
        _pos += _pos->_offsetEnd;
        return *this;
    }

    bool operator==(const ConstEntryIterator& rhs) const {
        return _pos == rhs._pos;
    }

    bool operator!=(const ConstEntryIterator& rhs) const {
        return !operator==(rhs);
    }

private:
    std::vector<MinMaxEntry>::const_iterator _pos;
};

class MinMaxObj {
public:
    MinMaxObj(std::vector<MinMaxEntry>& entries, std::vector<MinMaxEntry>::iterator pos)
        : _entries(entries), _pos(pos) {}

    MinMaxObj& operator=(const MinMaxObj& rhs) {
        _pos = rhs._pos;
        return *this;
    }

    MinMaxObj object(EntryIterator pos) const {
        return {_entries, pos._pos};
    }

    MinMaxObj parent() const {
        return {_entries, _pos - _pos->_offsetParent};
    }

    EntryIterator detach() const {
        return {_pos};
    }

    MinMaxElement& element() {
        return _pos->_element;
    }

    std::pair<EntryIterator, EntryIterator> insert(EntryIterator pos, std::string fieldName) {
        auto index = std::distance(_entries.begin(), _pos);
        auto inserted = _entries.emplace(pos._pos);
        _pos = _entries.begin() + index;


        inserted->_offsetEnd = 1;
        inserted->_element._fieldName = std::move(fieldName);
        inserted->_offsetParent = std::distance(_pos, inserted);

        auto it = inserted;
        auto parent = _pos;

        while (it != parent) {
            ++parent->_offsetEnd;

            auto next = EntryIterator(it);
            ++next;
            auto end = EntryIterator(parent + parent->_offsetEnd);
            for (; next != end; ++next) {
                ++next._pos->_offsetParent;
            }

            it = parent;
            parent = parent - parent->_offsetParent;
        }

        return std::make_pair(inserted, end());
    }

    EntryIterator begin() {
        return {_pos + 1};
    }

    EntryIterator end() {
        return {_pos + _pos->_offsetEnd};
    }

    ConstEntryIterator begin() const {
        return {_pos + 1};
    }

    ConstEntryIterator end() const {
        return {_pos + _pos->_offsetEnd};
    }

private:
    std::vector<MinMaxEntry>& _entries;
    std::vector<MinMaxEntry>::iterator _pos;
};

class MinMaxStore {
public:
    MinMaxStore() {
        auto& entry = entries.emplace_back();
        entry._offsetEnd = 1;
        entry._offsetParent = 0;
    }

    MinMaxObj root() {
        return {entries, entries.begin()};
    }

private:
    std::vector<MinMaxEntry> entries;
};
}
