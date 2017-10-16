/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/client/mongo_uri.h"

#include <utility>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/find_iterator.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/client/sasl_client_authenticate.h"
#include "mongo/db/namespace_string.h"
#include "mongo/util/dns_query.h"
#include "mongo/util/hex.h"
#include "mongo/util/mongoutils/str.h"

namespace {
constexpr std::array<char, 16> hexits{
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
const mongo::StringData kURIPrefix{"mongodb://"};
const mongo::StringData kURISRVPrefix{"mongodb+srv://"};
}  // namespace

/**
 * RFC 3986 Section 2.1 - Percent Encoding
 *
 * Encode data elements in a way which will allow them to be embedded
 * into a mongodb:// URI safely.
 */
void mongo::uriEncode(std::ostream& ss, StringData toEncode, StringData passthrough) {
    for (const auto& c : toEncode) {
        if ((c == '-') || (c == '_') || (c == '.') || (c == '~') || isalnum(c) ||
            (passthrough.find(c) != std::string::npos)) {
            ss << c;
        } else {
            // Encoding anything not included in section 2.3 "Unreserved characters"
            ss << '%' << hexits[(c >> 4) & 0xF] << hexits[c & 0xF];
        }
    }
}

mongo::StatusWith<std::string> mongo::uriDecode(StringData toDecode) {
    StringBuilder out;
    for (size_t i = 0; i < toDecode.size(); ++i) {
        const auto c = toDecode[i];
        if (c == '%') {
            if (i + 2 > toDecode.size()) {
                return Status(ErrorCodes::FailedToParse,
                              "Encountered partial escape sequence at end of string");
            }
            out << fromHex(toDecode.substr(i + 1, 2));
            i += 2;
        } else {
            out << c;
        }
    }
    return out.str();
}

namespace mongo {

namespace {

/**
 * Helper Method for MongoURI::parse() to split a string into exactly 2 pieces by a char
 * delimeter.
 */
std::pair<StringData, StringData> partitionForward(StringData str, const char c) {
    const auto delim = str.find(c);
    if (delim == std::string::npos) {
        return {str, StringData()};
    }
    return {str.substr(0, delim), str.substr(delim + 1)};
}

/**
 * Helper method for MongoURI::parse() to split a string into exactly 2 pieces by a char
 * delimiter searching backward from the end of the string.
 */
std::pair<StringData, StringData> partitionBackward(StringData str, const char c) {
    const auto delim = str.rfind(c);
    if (delim == std::string::npos) {
        return {StringData(), str};
    }
    return {str.substr(0, delim), str.substr(delim + 1)};
}

class FailedToParseException : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * Breakout method for parsing application/x-www-form-urlencoded option pairs
 *
 * foo=bar&baz=qux&...
 */
MongoURI::OptionsMap parseOptions(StringData options, StringData url) {
    MongoURI::OptionsMap ret;
    if (options.empty()) {
        return ret;
    }

    if (options.find('?') != std::string::npos) {
        throw FailedToParseException(
            str::stream() << "URI Cannot Contain multiple questions marks for mongodb:// URL: "
                          << url);
    }

    const auto optionsStr = options.toString();
    for (auto i =
             boost::make_split_iterator(optionsStr, boost::first_finder("&", boost::is_iequal()));
         i != std::remove_reference<decltype((i))>::type{};
         ++i) {
        const auto opt = boost::copy_range<std::string>(*i);
        if (opt.empty()) {
            throw FailedToParseException(
                str::stream() << "Missing a key/value pair in the options for mongodb:// URL: "
                              << url);
        }

        const auto kvPair = partitionForward(opt, '=');
        const auto keyRaw = kvPair.first;
        if (keyRaw.empty()) {
            throw FailedToParseException(
                str::stream()
                << "Missing a key for key/value pair in the options for mongodb:// URL: "
                << url);
        }
        const auto key = uriDecode(keyRaw);
        if (!key.isOK()) {
            throw FailedToParseException(
                str::stream() << "Key '" << keyRaw
                              << "' in options cannot properly be URL decoded for mongodb:// URL: "
                              << url);
        }
        const auto valRaw = kvPair.second;
        if (valRaw.empty()) {
            throw FailedToParseException(str::stream() << "Missing value for key '" << keyRaw
                                                       << "' in the options for mongodb:// URL: "
                                                       << url);
        }
        const auto val = uriDecode(valRaw);
        if (!val.isOK()) {
            throw FailedToParseException(
                str::stream() << "Value '" << valRaw << "' for key '" << keyRaw
                              << "' in options cannot properly be URL decoded for mongodb:// URL: "
                              << url);
        }

        ret[key.getValue()] = val.getValue();
    }

    return ret;
}

MongoURI::OptionsMap injectTXTOptions(MongoURI::OptionsMap options,
                                      const std::string& host,
                                      const StringData url,
                                      const bool seedlist) {
    // If there is no seedlist mode, then don't inject any TXT options.
    if (!seedlist)
        return options;

    // Get all TXT records and parse them as options, adding them to the options set.
    const auto txtRecords = dns::getTXTRecord(host);

    for (const auto& record : txtRecords) {
        auto txtOptions = parseOptions(record, url);
        // Note that, `std::map` and `std::unordered_map` insert does not replace existing
        // values -- this gives the desired behavior that user-specified values override TXT
        // record specified values.
        options.insert(begin(txtOptions), end(txtOptions));
    }

    return options;
}
}  // namespace

MongoURI MongoURI::parseImpl(const std::string& url) {
    const StringData urlSD(url);

    // 1. Validate and remove the scheme prefix mongodb://
    const bool seedlist = urlSD.startsWith(kURISRVPrefix);
    if (!(urlSD.startsWith(kURIPrefix) || seedlist)) {
        const auto cs_result = uassertStatusOK(ConnectionString::parse(url));
        return MongoURI(std::move(cs_result));
    }
    const auto uriWithoutPrefix = urlSD.substr(urlSD.find("://") + 3);

    // 2. Split the string by the first, unescaped / (if any), yielding:
    // split[0]: User information and host identifers
    // split[1]: Auth database and connection options
    const auto userAndDb = partitionForward(uriWithoutPrefix, '/');
    const auto userAndHostInfo = userAndDb.first;
    const auto databaseAndOptions = userAndDb.second;

    // 2.b Make sure that there are no question marks in the left side of the /
    //     as any options after the ? must still have the / delimeter
    if (databaseAndOptions.empty() && userAndHostInfo.find('?') != std::string::npos) {
        throw FailedToParseException(
            str::stream()
            << "URI must contain slash delimeter between hosts and options for mongodb:// URL: "
            << url);
    }

    // 3. Split the user information and host identifiers string by the last, unescaped @,
    // yielding:
    // split[0]: User information
    // split[1]: Host identifiers;
    const auto userAndHost = partitionBackward(userAndHostInfo, '@');
    const auto userInfo = userAndHost.first;
    const auto hostIdentifiers = userAndHost.second;

    // 4. Validate, split (if applicable), and URL decode the user information, yielding:
    // split[0] = username
    // split[1] = password
    const auto userAndPass = partitionForward(userInfo, ':');
    const auto usernameSD = userAndPass.first;
    const auto passwordSD = userAndPass.second;

    const auto containsColonOrAt = [](StringData str) {
        return (str.find(':') != std::string::npos) || (str.find('@') != std::string::npos);
    };

    if (containsColonOrAt(usernameSD)) {
        throw FailedToParseException(
            str::stream() << "Username must be URL Encoded for mongodb:// URL: " << url);
    }

    if (containsColonOrAt(passwordSD)) {
        throw FailedToParseException(
            str::stream() << "Password must be URL Encoded for mongodb:// URL: " << url);
    }

    // Get the username and make sure it did not fail to decode
    const auto usernameWithStatus = uriDecode(usernameSD);
    if (!usernameWithStatus.isOK()) {
        throw FailedToParseException(
            str::stream() << "Username cannot properly be URL decoded for mongodb:// URL: " << url);
    }
    const auto username = usernameWithStatus.getValue();

    // Get the password and make sure it did not fail to decode
    const auto passwordWithStatus = uriDecode(passwordSD);
    if (!passwordWithStatus.isOK())
        throw FailedToParseException(
            str::stream() << "Password cannot properly be URL decoded for mongodb:// URL: " << url);
    const auto password = passwordWithStatus.getValue();

    // 5. Validate, split, and URL decode the host identifiers.
    const auto hostIdentifiersStr = hostIdentifiers.toString();
    std::vector<HostAndPort> servers;
    for (auto i = boost::make_split_iterator(hostIdentifiersStr,
                                             boost::first_finder(",", boost::is_iequal()));
         i != std::remove_reference<decltype((i))>::type{};
         ++i) {
        const auto hostWithStatus = uriDecode(boost::copy_range<std::string>(*i));
        if (!hostWithStatus.isOK()) {
            throw FailedToParseException(
                str::stream() << "Host cannot properly be URL decoded for mongodb:// URL: " << url);
        }

        const auto host = hostWithStatus.getValue();
        if (host.empty()) {
            continue;
        }

        if ((host.find('/') != std::string::npos) && !StringData(host).endsWith(".sock")) {
            throw FailedToParseException(
                str::stream() << "'" << host << "' in '" << url
                              << "' appears to be a unix socket, but does not end in '.sock'");
        }

        servers.push_back(uassertStatusOK(HostAndPort::parse(host)));
    }
    if (servers.empty()) {
        throw FailedToParseException("No server(s) specified");
    }

    const std::string canonicalHost = servers.front().host();
    // If we're in seedlist mode, lookup the SRV record for `_mongodb._tcp` on the specified
    // domain name.  Take that list of servers as the new list of servers.
    if (seedlist) {
        if (servers.size() > 1) {
            throw FailedToParseException(
                "Only a single server may be specified with a mongo+srv:// url.");
        }
        auto srvEntries = dns::getSRVRecord("_mongodb._tcp." + canonicalHost);
        servers.clear();
        std::transform(begin(srvEntries), end(srvEntries), back_inserter(servers), [](auto& srv) {
            return HostAndPort(std::move(srv.host), srv.port);
        });
    }

    // 6. Split the auth database and connection options string by the first, unescaped ?,
    // yielding:
    // split[0] = auth database
    // split[1] = connection options
    const auto dbAndOpts = partitionForward(databaseAndOptions, '?');
    const auto databaseSD = dbAndOpts.first;
    const auto connectionOptions = dbAndOpts.second;
    const auto databaseWithStatus = uriDecode(databaseSD);
    if (!databaseWithStatus.isOK()) {
        throw FailedToParseException(str::stream() << "Database name cannot properly be URL "
                                                      "decoded for mongodb:// URL: "
                                                   << url);
    }
    const auto database = databaseWithStatus.getValue();

    // 7. Validate the database contains no prohibited characters
    // Prohibited characters:
    // slash ("/"), backslash ("\"), space (" "), double-quote ("""), or dollar sign ("$")
    // period (".") is also prohibited, but drivers MAY allow periods
    if (!database.empty() &&
        !NamespaceString::validDBName(database,
                                      NamespaceString::DollarInDbNameBehavior::Disallow)) {
        throw FailedToParseException(str::stream() << "Database name cannot have reserved "
                                                      "characters for mongodb:// URL: "
                                                   << url);
    }

    // 8. Validate, split, and URL decode the connection options
    auto options =
        injectTXTOptions(parseOptions(connectionOptions, url), canonicalHost, url, seedlist);

    // If a replica set option was specified, store it in the 'setName' field.
    const auto optIter = options.find("replicaSet");
    std::string setName;
    if (optIter != end(options)) {
        setName = optIter->second;
        invariant(!setName.empty());
    }

    ConnectionString cs(
        setName.empty() ? ConnectionString::MASTER : ConnectionString::SET, servers, setName);
    return MongoURI(std::move(cs), username, password, database, std::move(options));
}

StatusWith<MongoURI> MongoURI::parse(const std::string& url) try {
    return parseImpl(url);
}

catch (const std::exception&) {
    return exceptionToStatus();
}
}  // namespace mongo
