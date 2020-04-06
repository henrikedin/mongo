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

#include "mongo/util/shared_buffer.h"

namespace mongo {

/**
 * Immutable view of a fragment of a ref-counted buffer.
 * Shares reference count with the underlying buffer.
 */
class SharedBufferFragment {
public:
    SharedBufferFragment() : _offset(0), _size(0) {}
    explicit SharedBufferFragment(SharedBuffer buffer, size_t size)
        : _buffer(std::move(buffer)), _offset(0), _size(size) {}
    explicit SharedBufferFragment(SharedBuffer buffer, ptrdiff_t offset, size_t size)
        : _buffer(std::move(buffer)), _offset(offset), _size(size) {}

    void swap(SharedBufferFragment& other) {
        _buffer.swap(other._buffer);
        std::swap(_offset, other._offset);
        std::swap(_size, other._size);
    }

    const char* get() const {
        return _buffer.get() + _offset;
    }

    size_t size() const {
        return _size;
    }

    explicit operator bool() const {
        return (bool)_buffer;
    }

    /**
     * Returns true if this object has exclusive access to the underlying buffer.
     * (That is, reference count == 1).
     */
    bool isShared() const {
        return _buffer.isShared();
    }

    /**
     * Returns the allocation size of the underlying buffer.
     */
    size_t underlyingCapacity() const {
        return _buffer.capacity();
    }

private:
    SharedBuffer _buffer;
    ptrdiff_t _offset;
    size_t _size;
};

class SharedBufferFragmentBuilder {
public:
    SharedBufferFragmentBuilder(size_t blockSize) : _offset(0), _blockSize(blockSize) {}
    size_t start(size_t initialSize) {
        if (_buffer.capacity() < (_offset + initialSize)) {
            size_t allocSize = std::max(_blockSize, initialSize);
            _buffer = SharedBuffer::allocate(allocSize);
            _offset = 0;
        }
        return capacity();
    }
    size_t grow(size_t size) {
        auto currentCapacity = capacity();
        if (currentCapacity < size) {
            size_t allocSize = std::max(_blockSize, size);
            auto newBuffer = SharedBuffer::allocate(allocSize);
            memcpy(newBuffer.get(), _buffer.get() + _offset, currentCapacity);
            _buffer = std::move(newBuffer);
            _offset = 0;
        }

        return capacity();
    }

    SharedBufferFragment finish(size_t totalSize) {
        SharedBufferFragment fragment(_buffer, _offset, totalSize);
        _offset += totalSize;
        return fragment;
    }

    void discard() {
        if (_offset == 0)
            _buffer = {};
    }

    size_t capacity() const {
        return _buffer.capacity() - _offset;
    }

    char* get() const {
        return _buffer.get() + _offset;
    }

private:
    SharedBuffer _buffer;
    ptrdiff_t _offset;
    size_t _blockSize;
};


}  // namespace mongo
