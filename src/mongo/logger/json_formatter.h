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

#include "mongo/logger/attribute_payload.h"
#include "mongo/logger/log_component.h"
#include "mongo/logger/log_severity.h"
#include "mongo/util/time_support.h"

#include <fmt/format.h>

namespace mongo {
namespace logger {

class JsonFormatter {
public:
    static bool binary() {
        return false;
    };

    void operator()(boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
        using namespace boost::log;

        typedef fmt::basic_format_args<typename fmt::buffer_context<char>::type> formatter_args_t;

        // const std::string* message = extract<std::string>(attributes::message(), rec).get_ptr();
        // const auto args = extract<formatter_args_t>("formatting", rec).get_ptr();
        typedef boost::mpl::vector<int,
                                   std::string,
                                   StringData,
                                   LogSeverity,
                                   LogComponent,
                                   Date_t,
                                   formatter_args_t,
                                   AttributePayload>
            types;

        struct json_visitor {
            //using arg_formatter =
            //    fmt::arg_formatter<fmt::back_insert_range<fmt::internal::buffer<char>>>;

            //class custom_arg_formatter : public arg_formatter {
            //public:
            //    custom_arg_formatter(fmt::format_context& ctx,
            //                         fmt::basic_parse_context<char>* parse_ctx = nullptr,
            //                         fmt::format_specs* spec = nullptr)
            //        : arg_formatter(ctx, parse_ctx, spec), myctx(ctx), myparsectx(parse_ctx) {
            //        // map_.init()
            //    }

            //    using arg_formatter::operator();

            //    auto operator()(const char* value) {
            //        /*if (!specs_)
            //            return write(value), out();
            //        internal::handle_cstring_type_spec(specs_->type,
            //                                           cstring_spec_handler(*this, value));
            //        return out();*/

            //        return arg_formatter::operator()(value);
            //    }
            //    auto operator()(int value) {
            //        // if (spec().type() == 'x')
            //        //     return (*this)(
            //        //        static_cast<unsigned>(value));  // convert to unsigned and format
            //        return arg_formatter::operator()(value);
            //    }

            //private:
            //    fmt::format_context& myctx;
            //    fmt::basic_parse_context<char>* myparsectx;
            //    fmt::internal::arg_map<fmt::format_context> map_;
            //};

            json_visitor(boost::log::formatting_ostream& strm) : _strm(strm) {}

            typedef void result_type;

            result_type operator()(int val) const {
                _strm << val;
            }

            result_type operator()(std::string const& val) const {
                _strm << '\"' << val << '\"';
            }

            result_type operator()(StringData val) const {
                _strm << '\"' << val << '\"';
            }

            result_type operator()(LogSeverity val) const {
                _strm << '\"' << val << '\"';
            }

            result_type operator()(LogComponent val) const {
                _strm << '\"' << val << '\"';
            }

            result_type operator()(Date_t val) const {
                _strm << '\"' << val << '\"';
            }

            result_type operator()(formatter_args_t formatter_args) const {
				// prototype to try and get the name from named args, not possible without changes to libfmt

                //size_t size = formatter_args.max_size();
                //_strm << '{';
                //using namespace fmt::literals;
                //fmt::basic_memory_buffer<char> buffer;
                ///*fmt::internal::buffer<char>& internal_buffer = buffer;
                //fmt::internal::vformat_to<
                //    fmt::arg_formatter<fmt::back_insert_range<fmt::internal::buffer<char>>>>(
                //    internal_buffer, fmt::string_view("{type}"), formatter_args);*/
                //auto max_size = formatter_args.max_size();
                //for (unsigned int i = 0; i < max_size; ++i) {
                //    auto raw_arg = formatter_args.do_get(i);
                //    if (raw_arg.type() == fmt::internal::none_type)
                //        break;
                //    auto named_arg = static_cast<const fmt::v5::internal::named_arg_base<char>*>(
                //        raw_arg.value_.pointer);
                //}
                //auto raw_arg = formatter_args.do_get(0);
                //fmt::vformat_to<custom_arg_formatter>(
                //    buffer, fmt::string_view("{type}"), formatter_args);

                //auto str = fmt::to_string(buffer);
                //_strm << str;

               
                //_strm << '}';
            }

            result_type operator()(AttributePayload const& attributes) const {
                bool first = true;
                _strm << '{';
				for (unsigned int i = 0; i < attributes.names.size(); ++i)
				{
                    if (!first) {
                        _strm << ',';
                    }

					_strm << "\"" << attributes.names[i] << "\": ";
                    auto arg = attributes.values.get(i);
                    bool need_quotes = !arg.is_arithmetic();
					if (need_quotes)
                        _strm << '"';

					fmt::basic_memory_buffer<char> buffer;
                    fmt::vformat_to(
                        buffer, fmt::string_view(std::string("{") + std::to_string(i) + '}'), attributes.values);
                    _strm << fmt::to_string(buffer);
					if (need_quotes)
                        _strm << '"';
					first = false;
				}
                _strm << '}';
			}

        private:
            boost::log::formatting_ostream& _strm;
        };

        bool first = true;
        strm << '{';
        for (auto&& attr : rec.attribute_values()) {
            if (!first) {
                strm << ',';
            }
            strm << '\"' << attr.first << "\":";
            boost::log::visit<types>(attr.second, json_visitor(strm));
            first = false;
        }
        strm << '}';
    }
};

}  // namespace logger
}  // namespace mongo
