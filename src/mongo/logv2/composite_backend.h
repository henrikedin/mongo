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

#include <boost/log/detail/fake_mutex.hpp>
#include <boost/log/sinks/basic_sink_backend.hpp>
#include <boost/log/sinks/frontend_requirements.hpp>
#include <boost/log/sinks/unlocked_frontend.hpp>
#include <boost/make_shared.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <string>
#include <tuple>

namespace mongo {
namespace logv2 {

// has_requirement< typename sink_backend_type::frontend_requirements, synchronized_feeding >::value

template <typename Backend>
using backend_mutex_type = std::conditional_t<
    boost::log::sinks::has_requirement<typename Backend::frontend_requirements,
                                       boost::log::sinks::concurrent_feeding>::value,
    boost::log::aux::fake_mutex, boost::recursive_mutex>;

template <typename... Backend>
class composite_backend
    : public boost::log::sinks::
          basic_formatted_sink_backend<char, boost::log::sinks::combine_requirements< boost::log::sinks::concurrent_feeding, boost::log::sinks::flushing >::type> {
private:
    //! Base type
    typedef boost::log::sinks::
          basic_formatted_sink_backend<char, boost::log::sinks::combine_requirements< boost::log::sinks::concurrent_feeding, boost::log::sinks::flushing >::type>
        base_type;

public:
    //! Character type
    typedef typename base_type::char_type char_type;
    //! String type to be used as a message text holder
    typedef typename base_type::string_type string_type;
    //! Output stream type
    typedef std::basic_ostream<char_type> stream_type;


    composite_backend(boost::shared_ptr<Backend>&&... backends)
        : _backends(std::forward<boost::shared_ptr<Backend>>(backends)...)
    //: _backends(std::pair<boost::shared_ptr<Backend>, mutex_type<Backend>>(backends,
    // mutex_type<Backend>())...) {}
    {}

	/*template <typename Mutex, typename Backend>
	void flush_backend(Mutex& mutex, Backend& backend)
	{
	}*/

	template <typename Mutex, typename Backend>
	std::enable_if_t<boost::log::sinks::has_requirement<typename Backend::frontend_requirements,
                                       boost::log::sinks::flushing>::value>
	flush_backend(Mutex& mutex, Backend& backend)
	{
		boost::log::aux::exclusive_lock_guard<decltype(mutex)> lock(mutex);

		backend.flush();
	}

	template <typename Mutex, typename Backend>
	std::enable_if_t<!boost::log::sinks::has_requirement<typename Backend::frontend_requirements,
                                       boost::log::sinks::flushing>::value>
	flush_backend(Mutex& mutex, Backend& backend)
	{
	}

	template <size_t I>
    void flush_at() {
        flush_backend(std::get<I>(_mutexes), *std::get<I>(_backends));
    }

    template <size_t... Is>
    void flush_all(std::index_sequence<Is...>) {
        (flush_at<Is>(), ...);
    }

    // The function consumes the log records that come from the frontend
    template <size_t I>
    void consume_at(boost::log::record_view const& rec, string_type const& formatted_string) {
		boost::log::aux::exclusive_lock_guard< std::tuple_element_t<I, decltype(_mutexes)> > lock(std::get<I>(_mutexes));

        std::get<I>(_backends)->consume(rec, formatted_string);
    }

    template <size_t... Is>
    void consume_all(boost::log::record_view const& rec,
                     string_type const& formatted_string,
                     std::index_sequence<Is...>) {
        (consume_at<Is>(rec, formatted_string), ...);
    }

	template <typename F, size_t I>
    void apply_at(F&& func) {
		boost::log::aux::exclusive_lock_guard< std::tuple_element_t<I, decltype(_mutexes)> > lock(std::get<I>(_mutexes));

		func(*std::get<I>(_backends));
    }

    template <typename F, size_t... Is>
    void apply_all(F&& func, std::index_sequence<Is...>) {
        (apply_at<F, Is>(std::forward<F&&>(func)), ...);
    }

    void consume(boost::log::record_view const& rec, string_type const& formatted_string) {
        apply_all([&](auto& backend) { backend.consume(rec, formatted_string); },
                  std::make_index_sequence<sizeof...(Backend)>{});
        //consume_all(rec, formatted_string, std::make_index_sequence<sizeof...(Backend)>{});
    }

	void flush()
	{
		flush_all(std::make_index_sequence<sizeof...(Backend)>{});
		/*apply_all([&](auto& backend) { backend.flush(); },
                  std::make_index_sequence<sizeof...(Backend)>{});*/
	}

private:
    // std::tuple<std::pair<boost::shared_ptr<Backend>, mutex_type<Backend>>...> _backends;
    std::tuple<boost::shared_ptr<Backend>...> _backends;
    std::tuple<backend_mutex_type<Backend>...> _mutexes;

};

}  // namespace logv2
}  // namespace mongo
