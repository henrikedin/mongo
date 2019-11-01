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
#include "mongo/logv2/formatting_ostream_iterator.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/logv2/log_tag.h"
#include "mongo/logv2/visitor.h"
#include "mongo/util/time_support.h"

#include <fmt/format.h>
#include <stack>

namespace mongo {
namespace logv2 {
namespace {

class JsonFormattingVisitor final : public FormattingVisitor {
public:
    JsonFormattingVisitor(boost::log::formatting_ostream& strm) : _strm(strm) {}

    void write_int32(StringData name, int32_t val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_uint32(StringData name, uint32_t val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_int64(StringData name, int64_t val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_uint64(StringData name, uint64_t val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_bool(StringData name, bool val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_char(StringData name, char val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_double(StringData name, double val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_long_double(StringData name, long double val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "{}", val);
    }
    void write_string(StringData name, mongo::StringData val) override {
        write_name_internal(name);
        fmt::format_to(formatting_ostream_iterator(_strm), "\"{}\"", val);
    }
    void write_name(StringData name) override {
        write_name_internal(name);
    }
    void object_begin() override {
        _strm.put('{');
        _separator.push(""_sd);
    }
    void object_end() override {
        _strm.put('}');
        _separator.pop();
    }
    void array_begin() override {
        _strm.put('[');
        _separator.push(""_sd);
    }
    void array_end() override {
        _strm.put(']');
        _separator.pop();
    }

private:
    void write_name_internal(StringData name) {
        fmt::format_to(formatting_ostream_iterator(_strm), "{}\"{}\":", _separator.top(), name);
        _separator.top() = ","_sd;
    }

    boost::log::formatting_ostream& _strm;
    std::stack<StringData, std::vector<StringData>> _separator;
};
}  // namespace

class JsonFormatter {
public:
    static bool binary() {
        return false;
    };

    void operator()(boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
        using namespace boost::log;

        // Build a JSON object for the user attributes.
        const auto& attrs = extract<AttributeArgumentSet>(attributes::attributes(), rec).get();

        std::string id;
        auto stable_id = extract<StringData>(attributes::stableId(), rec).get();
        if (!stable_id.empty()) {
            id = fmt::format("\"id\":\"{}\",", stable_id);
        }

        std::string message;
        fmt::memory_buffer buffer;
        std::vector<fmt::basic_format_arg<fmt::format_context>> name_args;
        for (auto&& attr_name : attrs._names) {
            name_args.emplace_back(fmt::internal::make_arg<fmt::format_context>(attr_name));
        }
        fmt::vformat_to<NamedArgFormatter, char>(
            buffer,
            extract<StringData>(attributes::message(), rec).get().toString(),
            fmt::basic_format_args<fmt::format_context>(name_args.data(), name_args.size()));
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

        fmt::format_to(formatting_ostream_iterator(strm),
            "{{\"t\":\"{}\",\"s\":\"{}\"{: <{}}\"c\":\"{}\"{: "
            "<{}}\"ctx\":\"{}\",{}\"msg\":\"{}\"{}",
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
            attrs._names.empty() ? "" : ",\"attr\":");

        if (!attrs._names.empty()) {
            JsonFormattingVisitor visitor(strm);
            visitor.object_begin();
            attrs._values2.format(&visitor);
            visitor.object_end();
		}
        
		fmt::format_to(formatting_ostream_iterator(strm), "{}}}", tag);
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
