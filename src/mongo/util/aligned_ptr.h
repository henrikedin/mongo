/**
* Copyright (C) 2017 MongoDB Inc.
*
* This program is free software: you can redistribute it and/or  modify
* it under the terms of the GNU Affero General Public License, version 3,
* as published by the Free Software Foundation.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
* As a special exception, the copyright holders give permission to link the
* code of portions of this program with the OpenSSL library under certain
* conditions as described in each individual source file and distribute
* linked combinations including the program with the OpenSSL library. You
* must comply with the GNU Affero General Public License in all respects
* for all of the code used other than as permitted herein. If you modify
* file(s) with this exception, you may extend this exception to your
* version of the file(s), but you are not obligated to do so. If you do not
* wish to do so, delete this exception statement from your version. If you
* delete this exception statement from all source files in the program,
* then also delete it in the license file.
*/

#pragma once

namespace mongo {
template <class T>
class unique_ptr_aligned {
public:
    template <class... Args>
    static unique_ptr_aligned<T> make(Args&&... args);

    explicit operator bool() const noexcept {
        return internal_;
    }

    T* get() const noexcept {
        return aligned_pointer_();
    }
    T& operator*() const noexcept {
        return *aligned_pointer_();
    }
    T* operator->() const noexcept {
        return aligned_pointer_();
    }

    void reset() noexcept {
        internal_.reset();
    }
    void swap(unique_ptr_aligned<T>& other) noexcept {
        internal_.swap(other.internal_);
    }

private:
    typedef std::aligned_storage<sizeof(T) + alignof(T) - 16, alignof(T)> storage_t;

    T* aligned_pointer_() const noexcept {
        void* memory = internal_.get();
        std::size_t memory_size = memory ? sizeof(storage_t::type) : 0;

        return reinterpret_cast<T*>(std::align(alignof(T), sizeof(T), memory, memory_size));
    }

    std::unique_ptr<typename storage_t::type> internal_;
};

template <class T>
template <class... Args>
unique_ptr_aligned<T> unique_ptr_aligned<T>::make(Args&&... args) {
    unique_ptr_aligned<T> ptr;
    ptr.internal_.reset(new storage_t::type());
    new (ptr.aligned_pointer_()) T(std::forward<Args>(args)...);
    return ptr;
}
}
