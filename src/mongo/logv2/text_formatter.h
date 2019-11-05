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

#include <boost/log/attributes/value_extraction.hpp>
#include <boost/log/core/record_view.hpp>
#include <boost/log/expressions/message.hpp>
#include <boost/log/utility/formatting_ostream.hpp>

#include "mongo/logv2/attribute_argument_set.h"
#include "mongo/logv2/attributes.h"
#include "mongo/logv2/formatting_ostream_iterator.h"
#include "mongo/logv2/log_component.h"
#include "mongo/logv2/log_severity.h"
#include "mongo/logv2/log_tag.h"
#include "mongo/util/time_support.h"

#include <fmt/format.h>
#include <stack>

namespace mongo {
namespace logv2 {

class TextFormatter {
public:
    class Visitor final : public FormattingVisitor {
    public:
		fmt::basic_format_args<fmt::basic_format_context<formatting_ostream_iterator<>, char>>
        format_args() {
            return {_args.data(), static_cast<unsigned>(_args.size())};
        }

    private:
        void write_int32(StringData name, int32_t val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_uint32(StringData name, uint32_t val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_int64(StringData name, int64_t val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_uint64(StringData name, uint64_t val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_bool(StringData name, bool val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_char(StringData name, char val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_double(StringData name, double val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            } else {
                write_name(name);
                _nested.top() += fmt::format("{}", val);
            }
        }
        void write_long_double(StringData name, long double val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_string(StringData name, mongo::StringData val) override {
            if (!_depth) {
                _args.emplace_back(
                    fmt::internal::make_arg<
                        fmt::basic_format_context<formatting_ostream_iterator<>, char>>(val));
            }
        }
        void write_name(StringData name) override {
            if (_depth) {
                _nested.top() += _separator.top().toString() + name.toString() + ": ";
                _separator.top() = ", "_sd;
            }
        }
        void object_begin() override {
            _nested.push("(");
            _separator.push(""_sd);
            ++_depth;
        }
        void object_end() override {
            --_depth;
            _nested.top() += ")";
            _storage.push_back(std::move(_nested.top()));
            _args.emplace_back(
                fmt::internal::make_arg<
                                fmt::basic_format_context<formatting_ostream_iterator<>, char>>(
                _storage.back()));
            _nested.pop();
            _separator.pop();
        }
        void array_begin() override {
            ++_depth;
        }
        void array_end() override {
            --_depth;
        }

        std::vector<
            fmt::basic_format_arg<fmt::basic_format_context<formatting_ostream_iterator<>, char>>>
            _args;
        std::list<std::string> _storage;
        std::stack<std::string> _nested;
        std::stack<StringData> _separator;
        int _depth = 0;
    };

    TextFormatter() = default;

    // Boost log synchronizes calls to a formatter within a backend sink. If this is copied for some
    // reason (to another backend sink), no need to copy the buffer. This is just storage so we
    // don't need to allocate this memory every time. A final solution should format directly into
    // the formatting_ostream.
    TextFormatter(TextFormatter const&) {}

    TextFormatter& operator=(TextFormatter const&) {
        return *this;
    }

    static bool binary() {
        return false;
    };

    void operator()(boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
        using namespace boost::log;

        StringData message = extract<StringData>(attributes::message(), rec).get();
        const auto& attrs = extract<AttributeArgumentSet>(attributes::attributes(), rec).get();

		Visitor visitor;
        attrs._values2.format(&visitor);

        fmt::format_to(
            formatting_ostream_iterator(strm),
            "{} {:<2} {:<8} [{}] {}",
            extract<Date_t>(attributes::timeStamp(), rec).get().toString(),
            extract<LogSeverity>(attributes::severity(), rec).get().toStringDataCompact(),
            extract<LogComponent>(attributes::component(), rec).get().getNameForLog(),
            extract<StringData>(attributes::threadName(), rec).get(),
            extract<LogTag>(attributes::tags(), rec)
                .get().has(LogTag::kStartupWarnings)
                ? "** WARNING: "_sd
                : ""_sd
        );

		fmt::internal::output_range<formatting_ostream_iterator<char, std::char_traits<char>>>
            the_range{formatting_ostream_iterator<char, std::char_traits<char>>(strm)};

		fmt::vformat_to<
            fmt::arg_formatter<fmt::internal::output_range<
                formatting_ostream_iterator<char, std::char_traits<char>>>>>(
            the_range, to_string_view(message), visitor.format_args());
    }

protected:
    fmt::memory_buffer _buffer;
};

}  // namespace logv2
}  // namespace mongo
