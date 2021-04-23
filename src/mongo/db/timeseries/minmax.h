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

#include <memory>
#include <string>
#include <vector>

namespace mongo {

class MinMaxValue
{
    std::unique_ptr<char[]> _value;
    uint32_t _size;
};

class MinMaxData {
    uint8_t type;
    bool updated;
    MinMaxValue* value;
};

class MinMaxEntry {
public:
    uint32_t _offsetEnd;
    uint32_t _offsetParent;
    int32_t _numSubEntries = -1;
    std::string _fieldName;
    MinMaxData _min;
    MinMaxData _max;
};

class MinMaxObj;
class EntryIterator {
    friend class MinMaxObj;

public:
    EntryIterator(std::vector<MinMaxEntry>::iterator pos, uint32_t offsetPrev)
        : _pos(pos), _offsetPrev(offsetPrev) {}

    EntryIterator& operator++() {
        _offsetPrev = _pos->_offsetEnd;
        _pos += _offsetPrev;
        return *this;
    }

    bool operator==(const EntryIterator& rhs) const {
        return _pos == rhs._pos;
    }

    bool operator!=(const EntryIterator& rhs) const {
        return !operator==(rhs);
    }

    EntryIterator prev() const {
        return {_pos - _offsetPrev, 0};
    }

private:
    std::vector<MinMaxEntry>::iterator _pos;
    uint32_t _offsetPrev;
};

class MinMaxObj {
public:
    MinMaxObj(std::vector<MinMaxEntry>& entries, std::vector<MinMaxEntry>::iterator pos)
        : _entries(entries), _pos(pos) {}

    std::pair<EntryIterator, EntryIterator> insert(EntryIterator pos, std::string fieldName) {
        auto indexPrev = std::distance(_entries.begin(), pos._pos - pos._offsetPrev);
        _pos = _entries.emplace(pos._pos);


        _pos->_offsetEnd = 1;
        _pos->_fieldName = std::move(fieldName);

        std::vector<MinMaxEntry>::iterator parent;
        auto prev = _entries.begin() + indexPrev;
        if (pos._offsetPrev > 1 || prev->_numSubEntries == -1) {
            parent = prev - prev->_offsetParent;
        } else {
            parent = prev;
        }
        _pos->_offsetParent = std::distance(parent, _pos);

        // back down in hierarchy to fix end
        auto it = parent;
        do {
            ++it->_offsetEnd;
            it -= it->_offsetParent;

        } while (it->_offsetParent);


        it = _pos;
        while (it != parent) {
            auto next = EntryIterator(it, 0);
            ++next;
            auto end = EntryIterator(parent + parent->_offsetEnd, 0);
            for (; next != end; ++next) {
                ++next._pos->_offsetParent;
            }

            it = parent;
            parent = parent - parent->_offsetParent;
        }


        EntryIterator inserted(_pos, pos._offsetPrev);
        return std::make_pair(inserted, end());
    }

    EntryIterator begin() {
        return {_pos + 1, 1};
    }

    EntryIterator end() {
        return {_pos + _pos->_offsetEnd, 0};
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
        entry._numSubEntries = 0;
    }

    MinMaxObj root() {
        return {entries, entries.begin()};
    }

private:
    std::vector<MinMaxEntry> entries;
};
}
