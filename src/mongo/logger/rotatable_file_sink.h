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

#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/make_shared.hpp>
#include <string>

#include "mongo/logger/severity_filter.h"

namespace mongo {
namespace logger {

template <typename Formatter>
static boost::shared_ptr<boost::log::sinks::synchronous_sink<boost::log::sinks::text_file_backend>>
create_rotatable_file_sink(std::string const& file_name, bool append) {
    using namespace boost::log;

    std::ios_base::openmode open_mode = std::ios_base::out;
    if (append)
        open_mode |= std::ios_base::app;
    if (Formatter::binary())
        open_mode |= std::ios_base::binary;

    auto backend = boost::make_shared<sinks::text_file_backend>(
        keywords::file_name = file_name, keywords::open_mode = open_mode
    );
    backend->auto_flush(true);
    // backend->set_open_handler

    auto sink = boost::make_shared<
        boost::log::sinks::synchronous_sink<boost::log::sinks::text_file_backend>>(
        boost::move(backend));

    sink->set_filter(SeverityFilter());
    sink->set_formatter(Formatter());

    return sink;
}

}  // namespace logger
}  // namespace mongo
