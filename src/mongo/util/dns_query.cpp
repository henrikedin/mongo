/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */
#include "mongo/util/dns_query.h"

// DNS Headers for POSIX/libresolv have to be included in a specific order
// clang-format off
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/nameser.h>
#include <resolv.h>
// clang-format on

#include <stdio.h>

#include <iostream>
#include <cassert>
#include <sstream>
#include <string>
#include <cstdint>
#include <vector>
#include <array>
#include <stdexcept>
#include <exception>

#include <boost/noncopyable.hpp>

using std::begin;
using std::end;

namespace mongo {
namespace dns {
namespace {
#ifndef MONGOC_HAVE_DNS_API

enum class DNSQueryClass {
    kInternet = ns_c_in,
};

enum class DNSQueryType {
    kSRV = ns_t_srv,
    kTXT = ns_t_txt,
    kAddress = ns_t_a,
};

class ResourceRecord {
private:
    std::string service;
    ns_rr resource_record;
    const std::uint8_t* answerStart;
    const std::uint8_t* answerEnd;
    int pos;

    void badRecord() const {
        std::ostringstream oss;
        oss << "Invalid record " << pos << " of SRV answer for \"" << service << "\": \""
            << strerror(errno) << "\"";
        throw DNSLookupException(oss.str());
    };

public:
    explicit ResourceRecord() = default;

    explicit ResourceRecord(std::string s, ns_msg& ns_answer, const int p)
        : service(std::move(s)),
          answerStart(ns_msg_base(ns_answer)),
          answerEnd(ns_msg_end(ns_answer)),
          pos(p) {
        if (ns_parserr(&ns_answer, ns_s_an, p, &resource_record))
            badRecord();
    }

    std::vector<std::uint8_t> rawData() const {
        const std::uint8_t* const data = ns_rr_rdata(resource_record);
        const std::size_t length = ns_rr_rdlen(resource_record);

        return {data, data + length};
    }

    std::string addressEntry() const {
        std::string rv;
        for (const std::uint8_t& ch : rawData()) {
            std::ostringstream oss;
            oss << int(ch);
            rv += oss.str() + ".";
        }
        rv.pop_back();
        return rv;
    }

    SRVHostEntry srvHostEntry() const {
        const std::uint8_t* const data = ns_rr_rdata(resource_record);
        const uint16_t port = ntohs(*reinterpret_cast<const short*>(data + 4));

        std::string name;
        name.resize(8192, '@');

        const auto size = dn_expand(answerStart, answerEnd, data + 6, &name[0], name.size());

        if (size < 1)
            badRecord();

        // Trim the expanded name
        name.resize(name.find('\0'));
        name += '.';

        // return by copy is equivalent to a `shrink_to_fit` and `move`.
        return {name, port};
    }
};

class DNSResponse {
private:
    std::string service;
    std::vector<std::uint8_t> data;
    ns_msg ns_answer;
    std::size_t nRecords;

public:
    explicit DNSResponse(std::string s, std::vector<std::uint8_t> d)
        : service(std::move(s)), data(std::move(d)) {
        if (ns_initparse(data.data(), data.size(), &ns_answer)) {
            std::ostringstream oss;
            oss << "Invalid SRV answer for \"" << service << "\"";
            throw DNSLookupException(oss.str());
        }

        nRecords = ns_msg_count(ns_answer, ns_s_an);

        if (!nRecords) {
            std::ostringstream oss;
            oss << "No SRV records for \"" << service << "\"";
            throw DNSLookupException(oss.str());
        }
    }

    class iterator : mongo::relops::hook {
    private:
        DNSResponse* response;
        int pos = 0;
        ResourceRecord record;

        friend DNSResponse;

        explicit iterator(DNSResponse* const r)
            : response(r), record(this->response->service, this->response->ns_answer, 0) {}

        explicit iterator(DNSResponse* const r, int p) : response(r), pos(p) {}

        void hydrate() {
            record = ResourceRecord(this->response->service, this->response->ns_answer, this->pos);
        }

        void advance() {
            ++this->pos;
        }

    public:
        auto make_equality_lens() const {
            return std::tie(this->response, this->pos);
        }

        auto make_strict_weak_order_lens() const {
            return std::tie(this->response, this->pos);
        }

        const ResourceRecord& operator*() {
            this->hydrate();
            return this->record;
        }

        const ResourceRecord* operator->() {
            this->hydrate();
            return &this->record;
        }

        iterator& operator++() {
            this->advance();
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            this->advance();
            return tmp;
        }
    };

    auto begin() {
        return iterator(this);
    }

    auto end() {
        return iterator(this, this->nRecords);
    }

    std::size_t size() const {
        return this->nRecords;
    }
};

/**
 * The `DNSQueryState` object represents the state of a DNS query interface, on Unix-like systems.
 */
class DNSQueryState : boost::noncopyable {
#ifdef MONGO_HAVE_RES_NQUERY

private:
    struct __res_state state;

public:
    ~DNSQueryState() {
#ifdef MONGO_HAVE_RES_NDESTROY
        res_ndestroy(&state);
#elif defined(MONGO_HAVE_RES_NCLOSE)
        res_nclose(&state);
#endif
    }

    DNSQueryState() : state() {
        res_ninit(&state);
    }

#endif

public:
    std::vector<std::uint8_t> raw_lookup(const std::string& service,
                                         const DNSQueryClass class_,
                                         const DNSQueryType type) {
        std::vector<std::uint8_t> result(65536);
#ifdef MONGO_HAVE_RES_NQUERY
        const int size =
            res_nsearch(&state, service.c_str(), int(class_), int(type), &result[0], result.size());
#else
        const int size =
            res_query(service.c_str(), int(class_), int(type), &result[0], result.size());
#endif

        if (size < 0) {
            std::ostringstream oss;
            oss << "Failed to look up service \"" << service << "\": " << strerror(errno);
            throw DNSLookupNotFoundException(oss.str());
        }
        result.resize(size);

        return result;
    }

    DNSResponse lookup(const std::string& service,
                       const DNSQueryClass class_,
                       const DNSQueryType type) {
        return DNSResponse(service, raw_lookup(service, class_, type));
    }
};

#else
enum class DNSQueryClass { kInternet };

enum class DNSQueryType { kSRV = DNS_TYPE_SRV, kTXT = DNS_TYPE_TXT, kAddress = DNS_TYPE_A };

class ResourceRecord {};

void freeDnsRecord(PDNS_RECORD r) {
    DnsRecordListFree(r, DnsFreeRecordList);
}
class DNSResponse {
private:
    std::shared_ptr<std::remove_pointer<PDNS_RECORD>::type> results;

public:
    explicit DNSResponse(const PDNS_RECORD r) : results(r, freeDnsRecord) {}

    class iterator {
    private:
        DNS_RECORD* record;

        void advance() {
            record = record->pNext;
        }

    public:
    };
};

class DNSQueryState {
public:
    DNSResponse lookup(const std::string& service,
                       const DNSQueryClass class_,
                       const DNSQueryType type) {
        PDNS_RECORD queryResults;
        auto ec = DnsQuery_UTF8(
            service.c_str(), type, DNS_QUERY_BYPASS_CACHE, nullptr, &queryResults, nullptr);
        if (ec) {
            std::string buffer;
            buffer.resize(64 * 1024);
            LPVOID msgBuf = &buffer[0];
            auto count = FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM || FORMAT_MESSAGE_IGNORE_INSERTS,
                                       nullptr,
                                       ec,
                                       MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                                       reinterpret_cast<LPTSTR>(&msgBuf),
                                       buffer.size(),
                                       nullptr);

            if (count)
                buffer.resize(count);
            else
                buffer = "Unknown error";
            throw DNSLookupNotFoundException("Failed to look up service \""s + service + "\": "s +
                                             buffer);
        }
        return DNSResponse{queryResults};
    }
};
#endif
}  // namespace
}  // namespace dns

/**
 * Returns a string with the IP address or domain name listed...
 */
std::string dns::getARecord(const std::string& service) {
    DNSQueryState dnsQuery;
    auto response = dnsQuery.lookup(service, DNSQueryClass::kInternet, DNSQueryType::kAddress);

    if (response.size() == 0) {
        throw DNSLookupException("Looking up " + service + " A record no results.");
    }

    if (response.size() > 1) {
        throw DNSLookupException("Looking up " + service + " A record returned multiple results.");
    }

    return begin(response)->addressEntry();
}

/**
 * Returns a vector containing SRVHost entries for the specified `service`.
 * Throws `std::runtime_error` if the DNS lookup fails, for any reason.
 */
std::vector<dns::SRVHostEntry> dns::getSRVRecord(const std::string& service) {
    DNSQueryState dnsQuery;

    auto response = dnsQuery.lookup(service, DNSQueryClass::kInternet, DNSQueryType::kSRV);

    std::vector<SRVHostEntry> rv;
    rv.reserve(response.size());

    std::transform(begin(response), end(response), back_inserter(rv), [](const auto& entry) {
        return entry.srvHostEntry();
    });
    return rv;
}

/**
 * Returns a string containing TXT entries for a specified service.
 * Throws `std::runtime_error` if the DNS lookup fails, for any reason.
 */
std::vector<std::string> dns::getTXTRecord(const std::string& service) {
    DNSQueryState dnsQuery;

    auto response = dnsQuery.lookup(service, DNSQueryClass::kInternet, DNSQueryType::kTXT);

    std::vector<std::string> rv;
    rv.reserve(response.size());

    std::transform(begin(response), end(response), back_inserter(rv), [](const auto& entry) {
        const auto data = entry.rawData();
        const std::size_t amt = data.front();
        const auto first = begin(data) + 1;
        return std::string(first, first + amt);
    });
    return rv;
}
}  // namespace mongo
