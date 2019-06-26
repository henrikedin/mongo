/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include <boost/log/attributes/value_visitation.hpp>
#include <boost/log/core/record_view.hpp>
#include <boost/log/expressions/message.hpp>
#include <boost/log/utility/formatting_ostream.hpp>

#include "mongo/logv2/attribute_argument_set.h"
#include "mongo/logv2/attributes.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/logv2/log_tag.h"
#include "mongo/util/time_support.h"

#include <fmt/format.h>

namespace mongo {
namespace logv2 {

class JsonFormatter {
public:
    static bool binary() {
        return false;
    };

    void operator()(boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
        using namespace boost::log;

        const auto& attrs = extract<AttributeArgumentSet>(attributes::attributes(), rec).get();
        std::stringstream ss;
        bool first = true;
        ss << "{";
        for (std::size_t i = 0; i < attrs.names.size(); ++i) {
            if (!first)
                ss << ",";
            first = false;
            bool is_string = attrs.values.get(i).type() == fmt::internal::type::string_type ||
                attrs.values.get(i).type() == fmt::internal::type::cstring_type;
            ss << "\"" << attrs.names[i] << "\":";
            if (is_string)
                ss << "\"";

            fmt::memory_buffer buffer;
            auto format_str = fmt::format(
                attrs.values.get(i).type() == fmt::internal::type::custom_type ? "{{{}:j}}"
                                                                               : "{{{}}}",
                i);
            fmt::vformat_to(buffer, format_str, attrs.values);
            ss << fmt::to_string(buffer);

            if (is_string)
                ss << "\"";
        }
        ss << "}";

        std::string id;
        auto stable_id = extract<StringData>(attributes::stable_id(), rec).get();
        if (!stable_id.empty()) {
            id = fmt::format("\"id\":\"{}\",", stable_id);
        }

        // super naive algorithm to convert back into named args, probably littered with bugs and
        // inefficiencies.
        std::string message;
        std::string msg_source = extract<StringData>(attributes::message(), rec).get().toString();
        std::size_t pos = 0;
        std::size_t subst_index = 0;
        while (true) {
            auto res = msg_source.find('{', pos);
            if (res == std::string::npos) {
                message += msg_source.substr(pos);
                break;
            }

            if (res + 1 < msg_source.size() || msg_source[res + 1] != '{') {
                message += msg_source.substr(pos, res + 1 - pos);
                size_t end = res;
                while (true) {
                    end = msg_source.find_first_of(":}", end + 1);
                    if (msg_source[end] == ':') {
                        std::size_t substr = end;
                        do {
                            substr = msg_source.find('}', substr + 1);
                        } while (substr + 1 < msg_source.size() && msg_source[substr + 1] == '}');

                        message += attrs.names[subst_index].toString();
                        message += msg_source.substr(end, substr + 1 - end);
                        pos = substr + 1;
                        break;
                    } else if (end + 1 < msg_source.size() || msg_source[end + 1] != '}') {
                        int index = -1;
                        auto index_str = msg_source.substr(res + 1, end - (res + 1));
                        if (index_str == "0")
                            index = 0;
                        else {
                            auto parsed_index = strtol(index_str.c_str(), nullptr, 10);
                            index = parsed_index != 0 ? parsed_index : -1;
                        }
                        message += attrs.names[index != -1 ? index : subst_index].toString();
                        message += '}';

                        pos = end + 1;
                        break;
                    }
                }
                subst_index += 1;
            } else {
                pos = res + 1;
            }
        }


        StringData severity =
            extract<LogSeverity>(attributes::severity(), rec).get().toStringDataCompact();
        StringData component =
            extract<LogComponent>(attributes::component(), rec).get().getNameForLog();
        std::string tag;
        LogTag tags = extract<LogTag>(attributes::tags(), rec).get();
        if (tags != LogTag::kNone) {
            tag = fmt::format(",\"tags\":{}", tags);
        }

        auto formatted_body = fmt::format(
            "{{\"t\":\"{}\",\"s\":\"{}\"{: <{}}\"c\":\"{}\"{: "
            "<{}}\"ctx\":\"{}\",{}\"msg\":\"{}\"{}{}{}}}",
            dateToISOStringUTC(extract<Date_t>(attributes::time_stamp(), rec).get()),
            severity,
            ",",
            3 - severity.size(),
            component,
            ",",
            9 - component.size(),
            extract<StringData>(attributes::thread_name(), rec).get(),
            id,
            message,
            attrs.names.empty() ? "" : ",\"attr\":",
            attrs.names.empty() ? "" : ss.str(),
            tag);
        strm << formatted_body;
    }
};

}  // namespace logv2
}  // namespace mongo
