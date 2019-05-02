/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#include <boost/log/attributes/function.hpp>
#include <boost/log/attributes/mutable_constant.hpp>
#include <boost/log/keywords/channel.hpp>
#include <boost/log/keywords/severity.hpp>
#include <boost/log/sources/basic_logger.hpp>
#include <boost/log/sources/threading_models.hpp>

#include "mongo/logger/log_component.h"
#include "mongo/logger/log_severity.h"

//
//#include <boost/log/attributes/constant.hpp>
//#include <boost/log/sources/basic_logger.hpp>
//#include <boost/log/sources/threading_models.hpp>
//#include <boost/log/sources/severity_feature.hpp>
//#include <boost/log/sources/channel_feature.hpp>
//#include <boost/log/utility/strictest_lock.hpp>
//
namespace mongo {
namespace logger {
namespace keywords {
BOOST_PARAMETER_KEYWORD(tag_ns, tag)
}
//
// template <typename BaseT>
// class record_tagger_feature : public BaseT {
// public:
//    // Let's import some types that we will need. These imports should be public,
//    // in order to allow other features that may derive from record_tagger to do the same.
//    typedef typename BaseT::char_type char_type;
//    typedef typename BaseT::threading_model threading_model;
//
// public:
//    // Default constructor. Initializes m_Tag to an invalid value.
//    record_tagger_feature();
//    // Copy constructor. Initializes m_Tag to a value, equivalent to that.m_Tag.
//    record_tagger_feature(record_tagger_feature const& that);
//    // Forwarding constructor with named parameters
//    template <typename ArgsT>
//    record_tagger_feature(ArgsT const& args);
//
//    // The method will require locking, so we have to define locking requirements for it.
//    // We use the strictest_lock trait in order to choose the most restricting lock type.
//    typedef typename boost::log::strictest_lock<boost::lock_guard<threading_model>,
//                                                typename BaseT::open_record_lock,
//                                                typename BaseT::add_attribute_lock,
//                                                typename BaseT::remove_attribute_lock>::type
//        open_record_lock;
//
// protected:
//    // Lock-less implementation of operations
//    template <typename ArgsT>
//    boost::log::record open_record_unlocked(ArgsT const& args);
//};
//
// template <typename BaseT>
// record_tagger_feature<BaseT>::record_tagger_feature() {}
//
// template <typename BaseT>
// record_tagger_feature<BaseT>::record_tagger_feature(record_tagger_feature const& that)
//    : BaseT(static_cast<BaseT const&>(that)) {}
//
// template <typename BaseT>
// template <typename ArgsT>
// record_tagger_feature<BaseT>::record_tagger_feature(ArgsT const& args) : BaseT(args) {}
//
// template <typename BaseT>
// template <typename ArgsT>
// boost::log::record record_tagger_feature<BaseT>::open_record_unlocked(ArgsT const& args) {
//    // Extract the named argument from the parameters pack
//    std::string tag_value = args[keywords::tag | std::string()];
//
//    boost::log::attribute_set& attrs = BaseT::attributes();
//    boost::log::attribute_set::iterator tag = attrs.end();
//    if (!tag_value.empty()) {
//        // Add the tag as a new attribute
//        std::pair<boost::log::attribute_set::iterator, bool> res = BaseT::add_attribute_unlocked(
//            "Tag", boost::log::attributes::constant<std::string>(tag_value));
//        if (res.second)
//            tag = res.first;
//    }
//
//    // In any case, after opening a record remove the tag from the attributes
//    ON_BLOCK_EXIT([&] {
//        if (tag != attrs.end())
//            attrs.erase(tag);
//    });
//
//    // Forward the call to the base feature
//    return BaseT::open_record_unlocked(args);
//}
//
//// A convenience metafunction to specify the feature
//// in the list of features of the final logger later
// struct record_tagger : public boost::mpl::quote1<record_tagger_feature> {};
//
// class logger
//    : public boost::log::sources::basic_composite_logger<char,
//                                         logger,
//                                           boost::log::sources::single_thread_model,
//                   boost::log::sources::features<boost::log::sources::severity<LogSeverity>,
//                                                 boost::log::sources::channel<LogComponent>,
//                                                 record_tagger>> {
//    // The following line will automatically generate forwarding constructors that
//    // will call to the corresponding constructors of the base class
//    BOOST_LOG_FORWARD_LOGGER_MEMBERS_TEMPLATE(logger)
//};

class logger : public boost::log::sources::
                   basic_logger<char, logger, boost::log::sources::single_thread_model> {
private:
private:
    typedef boost::log::sources::
        basic_logger<char, logger, boost::log::sources::single_thread_model>
            base_type;

public:
    logger() : _severity(LogSeverity::Log()), _component(LogComponent::kDefault)
	{
        add_attribute_unlocked("Severity", _severity);

        add_attribute_unlocked(
            "Channel", _component);

        add_attribute_unlocked("TimeStamp",
                               boost::log::attributes::make_function([]() { return Date_t::now(); }));

        add_attribute_unlocked(
            "ThreadName", boost::log::attributes::make_function([]() { return getThreadName(); }));
    }

    boost::log::record open_record(LogSeverity severity, LogComponent component) {
        // Perform a quick check first
        if (this->core()->get_logging_enabled())
		{
            _severity.set(severity);
            _component.set(component);
            return base_type::open_record_unlocked();
		}
        else
            return boost::log::record();
    }

    void push_record(BOOST_RV_REF(boost::log::record) rec) {
        base_type::push_record_unlocked(boost::move(rec));
        _severity.set(LogSeverity::Log());
        _component.set(LogComponent::kDefault);
    }

private:
    boost::log::attributes::mutable_constant<LogSeverity> _severity;
    boost::log::attributes::mutable_constant<LogComponent> _component;
};


}  // namespace logger
}  // namespace mongo
