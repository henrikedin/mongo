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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/util/file.h"

#include <boost/filesystem/operations.hpp>
#include <cstdint>
#include <iostream>
#include <string>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#endif

#include "mongo/logv2/log.h"
#include "mongo/platform/basic.h"
#include "mongo/util/allocator.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"
#include "mongo/util/text.h"

namespace mongo {

#if defined(_WIN32)

File::File() : _bad(true), _handle(INVALID_HANDLE_VALUE) {}

File::~File() {
    if (is_open()) {
        CloseHandle(_handle);
    }
    _handle = INVALID_HANDLE_VALUE;
}

intmax_t File::freeSpace(const std::string& path) {
    ULARGE_INTEGER avail;
    if (GetDiskFreeSpaceExW(toWideString(path.c_str()).c_str(),
                            &avail,      // bytes available to caller
                            nullptr,     // ptr to returned total size
                            nullptr)) {  // ptr to returned total free
        return avail.QuadPart;
    }
    DWORD dosError = GetLastError();
    LOGV2(23140,
          "In File::freeSpace(), GetDiskFreeSpaceEx for '{path}' failed with "
          "{errnoWithDescription_dosError}",
          "path"_attr = path,
          "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
    return -1;
}

void File::fsync() const {
    if (FlushFileBuffers(_handle) == 0) {
        DWORD dosError = GetLastError();
        LOGV2(23141,
              "In File::fsync(), FlushFileBuffers for '{name}' failed with "
              "{errnoWithDescription_dosError}",
              "name"_attr = _name,
              "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
    }
}

bool File::is_open() const {
    return _handle != INVALID_HANDLE_VALUE;
}

fileofs File::len() {
    LARGE_INTEGER li;
    if (GetFileSizeEx(_handle, &li)) {
        return li.QuadPart;
    }
    _bad = true;
    DWORD dosError = GetLastError();
    LOGV2(23142,
          "In File::len(), GetFileSizeEx for '{name}' failed with {errnoWithDescription_dosError}",
          "name"_attr = _name,
          "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
    return 0;
}

void File::open(const char* filename, bool readOnly, bool direct) {
    _name = filename;
    _handle = CreateFileW(toNativeString(filename).c_str(),               // filename
                          (readOnly ? 0 : GENERIC_WRITE) | GENERIC_READ,  // desired access
                          FILE_SHARE_WRITE | FILE_SHARE_READ,             // share mode
                          nullptr,                                        // security
                          OPEN_ALWAYS,                                    // create or open
                          FILE_ATTRIBUTE_NORMAL,                          // file attributes
                          nullptr);                                       // template
    _bad = !is_open();
    if (_bad) {
        DWORD dosError = GetLastError();
        LOGV2(
            23143,
            "In File::open(), CreateFileW for '{name}' failed with {errnoWithDescription_dosError}",
            "name"_attr = _name,
            "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
    }
}

void File::read(fileofs o, char* data, unsigned len) {
    LARGE_INTEGER li;
    li.QuadPart = o;
    if (SetFilePointerEx(_handle, li, nullptr, FILE_BEGIN) == 0) {
        _bad = true;
        DWORD dosError = GetLastError();
        LOGV2(23144,
              "In File::read(), SetFilePointerEx for '{name}' tried to set the file pointer to {o} "
              "but failed with {errnoWithDescription_dosError}",
              "name"_attr = _name,
              "o"_attr = o,
              "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
        return;
    }
    DWORD bytesRead;
    if (!ReadFile(_handle, data, len, &bytesRead, 0)) {
        _bad = true;
        DWORD dosError = GetLastError();
        LOGV2(23145,
              "In File::read(), ReadFile for '{name}' failed with {errnoWithDescription_dosError}",
              "name"_attr = _name,
              "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
    } else if (bytesRead != len) {
        _bad = true;
        msgasserted(10438,
                    str::stream() << "In File::read(), ReadFile for '" << _name << "' read "
                                  << bytesRead << " bytes while trying to read " << len
                                  << " bytes starting at offset " << o << ", truncated file?");
    }
}

void File::truncate(fileofs size) {
    if (len() <= size) {
        return;
    }
    LARGE_INTEGER li;
    li.QuadPart = size;
    if (SetFilePointerEx(_handle, li, nullptr, FILE_BEGIN) == 0) {
        _bad = true;
        DWORD dosError = GetLastError();
        LOGV2(23146,
              "In File::truncate(), SetFilePointerEx for '{name}' tried to set the file pointer to "
              "{size} but failed with {errnoWithDescription_dosError}",
              "name"_attr = _name,
              "size"_attr = size,
              "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
        return;
    }
    if (SetEndOfFile(_handle) == 0) {
        _bad = true;
        DWORD dosError = GetLastError();
        LOGV2(23147,
              "In File::truncate(), SetEndOfFile for '{name}' failed with "
              "{errnoWithDescription_dosError}",
              "name"_attr = _name,
              "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
    }
}

void File::write(fileofs o, const char* data, unsigned len) {
    LARGE_INTEGER li;
    li.QuadPart = o;
    if (SetFilePointerEx(_handle, li, nullptr, FILE_BEGIN) == 0) {
        _bad = true;
        DWORD dosError = GetLastError();
        LOGV2(23148,
              "In File::write(), SetFilePointerEx for '{name}' tried to set the file pointer to "
              "{o} but failed with {errnoWithDescription_dosError}",
              "name"_attr = _name,
              "o"_attr = o,
              "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
        return;
    }
    DWORD bytesWritten;
    if (WriteFile(_handle, data, len, &bytesWritten, nullptr) == 0) {
        _bad = true;
        DWORD dosError = GetLastError();
        LOGV2(23149,
              "In File::write(), WriteFile for '{name}' tried to write {len} bytes but only wrote "
              "{bytesWritten} bytes, failing with {errnoWithDescription_dosError}",
              "name"_attr = _name,
              "len"_attr = len,
              "bytesWritten"_attr = bytesWritten,
              "errnoWithDescription_dosError"_attr = errnoWithDescription(dosError));
    }
}

#else  // _WIN32

File::File() : _bad(true), _fd(-1) {}

File::~File() {
    if (is_open()) {
        ::close(_fd);
    }
    _fd = -1;
}

intmax_t File::freeSpace(const std::string& path) {
    struct statvfs info;
    if (statvfs(path.c_str(), &info) == 0) {
        return static_cast<intmax_t>(info.f_bavail) * info.f_frsize;
    }
    LOGV2(23150,
          "In File::freeSpace(), statvfs for '{path}' failed with {errnoWithDescription}",
          "path"_attr = path,
          "errnoWithDescription"_attr = errnoWithDescription());
    return -1;
}

void File::fsync() const {
    if (::fsync(_fd)) {
        LOGV2(23151,
              "In File::fsync(), ::fsync for '{name}' failed with {errnoWithDescription}",
              "name"_attr = _name,
              "errnoWithDescription"_attr = errnoWithDescription());
    }
}

bool File::is_open() const {
    return _fd > 0;
}

fileofs File::len() {
    off_t o = lseek(_fd, 0, SEEK_END);
    if (o != static_cast<off_t>(-1)) {
        return o;
    }
    _bad = true;
    LOGV2(23152,
          "In File::len(), lseek for '{name}' failed with {errnoWithDescription}",
          "name"_attr = _name,
          "errnoWithDescription"_attr = errnoWithDescription());
    return 0;
}

#ifndef O_NOATIME
#define O_NOATIME 0
#endif

void File::open(const char* filename, bool readOnly, bool direct) {
    _name = filename;
    _fd = ::open(filename,
                 (readOnly ? O_RDONLY : (O_CREAT | O_RDWR | O_NOATIME))
#if defined(O_DIRECT)
                     | (direct ? O_DIRECT : 0)
#endif
                     ,
                 S_IRUSR | S_IWUSR);
    _bad = !is_open();
    if (_bad) {
        LOGV2(23153,
              "In File::open(), ::open for '{name}' failed with {errnoWithDescription}",
              "name"_attr = _name,
              "errnoWithDescription"_attr = errnoWithDescription());
    }
}

void File::read(fileofs o, char* data, unsigned len) {
    ssize_t bytesRead = ::pread(_fd, data, len, o);
    if (bytesRead == -1) {
        _bad = true;
        LOGV2(23154,
              "In File::read(), ::pread for '{name}' failed with {errnoWithDescription}",
              "name"_attr = _name,
              "errnoWithDescription"_attr = errnoWithDescription());
    } else if (bytesRead != static_cast<ssize_t>(len)) {
        _bad = true;
        msgasserted(16569,
                    str::stream() << "In File::read(), ::pread for '" << _name << "' read "
                                  << bytesRead << " bytes while trying to read " << len
                                  << " bytes starting at offset " << o << ", truncated file?");
    }
}

void File::truncate(fileofs size) {
    if (len() <= size) {
        return;
    }
    if (ftruncate(_fd, size) != 0) {
        _bad = true;
        LOGV2(23155,
              "In File::truncate(), ftruncate for '{name}' tried to set the file pointer to {size} "
              "but failed with {errnoWithDescription}",
              "name"_attr = _name,
              "size"_attr = size,
              "errnoWithDescription"_attr = errnoWithDescription());
        return;
    }
}

void File::write(fileofs o, const char* data, unsigned len) {
    ssize_t bytesWritten = ::pwrite(_fd, data, len, o);
    if (bytesWritten != static_cast<ssize_t>(len)) {
        _bad = true;
        LOGV2(23156,
              "In File::write(), ::pwrite for '{name}' tried to write {len} bytes but only wrote "
              "{bytesWritten} bytes, failing with {errnoWithDescription}",
              "name"_attr = _name,
              "len"_attr = len,
              "bytesWritten"_attr = bytesWritten,
              "errnoWithDescription"_attr = errnoWithDescription());
    }
}

#endif  // _WIN32
}  // namespace mongo
