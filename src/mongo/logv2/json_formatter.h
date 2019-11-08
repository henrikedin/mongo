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

#include "mongo/bson/bsonobj.h"
#include "mongo/logv2/attribute_storage.h"
#include "mongo/logv2/attributes.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/logv2/log_tag.h"
#include "mongo/util/time_support.h"

#include <fmt/format.h>

namespace mongo {
namespace logv2 {

namespace detail {
struct JsonValueExtractor {
    JsonValueExtractor() : _separator(""_sd) {}
    void operator()(StringData name, CustomAttributeValue const& val) {
        if (val._toBSON) {
            _storage.push_back(val._toBSON().jsonString());

            // This is a JSON subobject, select overload that does not surround value with quotes
            operator()(name, _storage.back());
        } else {
            _storage.push_back(val._toString());

            // This is a string, select overload that surrounds value with quotes
            operator()(name, StringData(_storage.back()));
        }
    }

    void operator()(StringData name, StringData value) {
        store(R"({}"{}":"{}")", name, value);
    }

    template <typename T>
    void operator()(StringData name, T&& value) {
        store(R"({}"{}":{})", name, value);
    }

    template <typename T>
    void store(const char* fmt_str, StringData name, T&& value) {
        _name_args.emplace_back(fmt::internal::make_arg<fmt::format_context>(name));
        fmt::format_to(_buffer, fmt_str, _separator, name, value);
        _separator = ","_sd;
    }

    fmt::memory_buffer _buffer;
    std::vector<fmt::basic_format_arg<fmt::format_context>> _name_args;
    std::list<std::string> _storage;
    StringData _separator;
};
}  // namespace detail

class JsonFormatter {
public:
    static bool binary() {
        return false;
    };

    void operator()(boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
        using namespace boost::log;

        // Build a JSON object for the user attributes.
        const auto& attrs =
            extract<TypeErasedAttributeStorage>(attributes::attributes(), rec).get();

        detail::JsonValueExtractor extractor;
        attrs.apply(extractor);

        std::string id;
        auto stable_id = extract<StringData>(attributes::stableId(), rec).get();
        if (!stable_id.empty()) {
            id = fmt::format("\"id\":\"{}\",", stable_id);
        }

        std::string message;
        fmt::memory_buffer buffer;
        fmt::vformat_to<NamedArgFormatter, char>(
            buffer,
            extract<StringData>(attributes::message(), rec).get().toString(),
            fmt::basic_format_args<fmt::format_context>(extractor._name_args.data(),
                                                        extractor._name_args.size()));
        message = fmt::to_string(buffer);

        StringData severity =
            extract<LogSeverity>(attributes::severity(), rec).get().toStringDataCompact();
        StringData component =
            extract<LogComponent>(attributes::component(), rec).get().getNameForLog();
        std::string tag;
        LogTag tags = extract<LogTag>(attributes::tags(), rec).get();
        if (tags != LogTag::kNone) {
            tag = fmt::format(",\"tags\":{}", tags.toJSONArray());
        }

        auto formatted_body =
            fmt::format(R"({{)"
                        R"("t":"{}",)"        // timestamp
                        R"("s":"{}"{: <{}})"  // severity with padding for the comma
                        R"("c":"{}"{: <{}})"  // component with padding for the comma
                        R"("ctx":"{}",)"      // context
                        R"({})"               // optional stable id
                        R"("msg":"{}")"       // message
                        R"({})"               // optional attribute key
                        R"({})"               // optional attribute values
                        R"({})"               // optional attribute closing
                        R"({})"               // optional tags
                        R"(}})",
                        dateToISOStringUTC(extract<Date_t>(attributes::timeStamp(), rec).get()),
                        severity,
                        ",",
                        3 - severity.size(),
                        component,
                        ",",
                        9 - component.size(),
                        extract<StringData>(attributes::threadName(), rec).get(),
                        id,
                        message,
                        attrs.empty() ? "" : ",\"attr\":{",
                        attrs.empty() ? "" : fmt::to_string(extractor._buffer),
                        attrs.empty() ? "" : "}",
                        tag);
        strm << formatted_body;
    }

private:
    class NamedArgFormatter : public fmt::arg_formatter<fmt::internal::buffer_range<char>> {
    public:
        typedef fmt::arg_formatter<fmt::internal::buffer_range<char>> arg_formatter;

        NamedArgFormatter(fmt::format_context& ctx,
                          fmt::basic_parse_context<char>* parse_ctx = nullptr,
                          fmt::format_specs* spec = nullptr)
            : arg_formatter(ctx, parse_ctx, nullptr) {
            // Pretend that we have no format_specs, but store so we can re-construct the format
            // specifier
            if (spec) {
                spec_ = *spec;
            }
        }


        using arg_formatter::operator();

        auto operator()(fmt::string_view name) {
            using namespace fmt;

            write("{");
            arg_formatter::operator()(name);
            // If formatting spec was provided, reconstruct the string. Libfmt does not provide this
            // unfortunately
            if (spec_) {
                char str[2] = {'\0', '\0'};
                write(":");
                if (spec_->fill() != ' ') {
                    str[0] = static_cast<char>(spec_->fill());
                    write(str);
                }

                switch (spec_->align()) {
                    case ALIGN_LEFT:
                        write("<");
                        break;
                    case ALIGN_RIGHT:
                        write(">");
                        break;
                    case ALIGN_CENTER:
                        write("^");
                        break;
                    case ALIGN_NUMERIC:
                        write("=");
                        break;
                    default:
                        break;
                };

                if (spec_->has(PLUS_FLAG))
                    write("+");
                else if (spec_->has(MINUS_FLAG))
                    write("-");
                else if (spec_->has(SIGN_FLAG))
                    write(" ");
                else if (spec_->has(HASH_FLAG))
                    write("#");

                if (spec_->align() == ALIGN_NUMERIC && spec_->fill() == 0)
                    write("0");

                if (spec_->width() > 0)
                    write(std::to_string(spec_->width()).c_str());

                if (spec_->has_precision()) {
                    write(".");
                    write(std::to_string(spec_->precision).c_str());
                }
                str[0] = spec_->type;
                write(str);
            }
            write("}");
            return out();
        }

    private:
        boost::optional<fmt::format_specs> spec_;
    };
};

}  // namespace logv2
}  // namespace mongo
