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

#include "mongo/logger/log_component.h"
#include "mongo/logger/log_severity.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace logger {

class JsonFormatter {
public:
    static bool binary() {
        return false;
    };

    void operator()(boost::log::record_view const& rec, boost::log::formatting_ostream& strm) {
        using namespace boost::log;

        typedef boost::mpl::vector<int, std::string, StringData, LogSeverity, LogComponent, Date_t> types;

        struct json_visitor {
            json_visitor(boost::log::formatting_ostream& strm) : _strm(strm) {}

            typedef void result_type;

            result_type operator()(int val) const {
                _strm << val;
            }

            result_type operator()(std::string const& val) const {
                _strm << val;
            }

			result_type operator()(StringData val) const {
                _strm << val;
            }

			result_type operator()(LogSeverity val) const {
                _strm << val;
            }

			result_type operator()(LogComponent val) const {
                _strm << val;
            }

			result_type operator()(Date_t val) const {
                _strm << val;
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
            strm << '\"' << attr.first << "\":\"";
            boost::log::visit<types>(attr.second, json_visitor(strm));
            strm << '\"';
            first = false;
        }
        strm << '}';
    }
};

}  // namespace logger
}  // namespace mongo
