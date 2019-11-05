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

#include <boost/log/utility/formatting_ostream.hpp>
#include <iterator>

namespace mongo::logv2 {

template <class CharT = char, class Traits = std::char_traits<CharT>>
class formatting_ostream_iterator {
public:
    using iterator_category = std::output_iterator_tag;
    using value_type = CharT;
    using difference_type = void;
    using pointer = void;
    using reference = void;

    using char_type = CharT;
    using traits_type = Traits;
    using ostream_type = boost::log::basic_formatting_ostream<CharT, Traits>;

    formatting_ostream_iterator(ostream_type& ostrm)
        : ostrm_(&ostrm) {  // construct from output stream and delimiter
    }

    formatting_ostream_iterator& operator=(const CharT& val) {  // insert value into output stream
        ostrm_->put(val);
        return *this;
    }

    formatting_ostream_iterator& operator*() {  // pretend to return designated value
        return *this;
    }

    formatting_ostream_iterator& operator++() {  // pretend to preincrement
        return *this;
    }

    formatting_ostream_iterator& operator++(int) {  // pretend to postincrement
        return *this;
    }

protected:
    ostream_type* ostrm_;
};

}  // namespace mongo::logv2
