//
// impl/io_context.ipp
// ~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2017 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ASIO_IMPL_IO_POOL_CONTEXT_IPP
#define ASIO_IMPL_IO_POOL_CONTEXT_IPP

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
# pragma once
#endif // defined(_MSC_VER) && (_MSC_VER >= 1200)

#include "asio/detail/config.hpp"
#include "asio/io_pool_context.hpp"
#include "asio/detail/concurrency_hint.hpp"
#include "asio/detail/limits.hpp"
#include "asio/detail/scoped_ptr.hpp"
#include "asio/detail/service_registry.hpp"
#include "asio/detail/throw_error.hpp"

#if defined(ASIO_HAS_IOCP)
# include "asio/detail/win_iocp_io_pool_context.hpp"
#else
# include "asio/detail/scheduler.hpp"
#endif

#include "asio/detail/push_options.hpp"

namespace asio {

io_pool_context::io_pool_context()
  : impl_(add_impl(new impl_type(*this, ASIO_CONCURRENCY_HINT_DEFAULT)))
{
}

io_pool_context::io_pool_context(int concurrency_hint)
  : impl_(add_impl(new impl_type(*this, concurrency_hint == 1
          ? ASIO_CONCURRENCY_HINT_1 : concurrency_hint)))
{
}

io_pool_context::impl_type& io_pool_context::add_impl(io_pool_context::impl_type* impl)
{
  asio::detail::scoped_ptr<impl_type> scoped_impl(impl);
  asio::add_service<impl_type>(*this, scoped_impl.get());
  return *scoped_impl.release();
}

io_pool_context::~io_pool_context()
{
}

io_pool_context::service::service(asio::io_pool_context& owner)
  : execution_context::service(owner)
{
}

io_pool_context::service::~service()
{
}

void io_pool_context::service::shutdown()
{
#if !defined(ASIO_NO_DEPRECATED)
  shutdown_service();
#endif // !defined(ASIO_NO_DEPRECATED)
}

#if !defined(ASIO_NO_DEPRECATED)
void io_pool_context::service::shutdown_service()
{
}
#endif // !defined(ASIO_NO_DEPRECATED)

void io_pool_context::service::notify_fork(io_context::fork_event ev)
{
#if !defined(ASIO_NO_DEPRECATED)
  fork_service(ev);
#else // !defined(ASIO_NO_DEPRECATED)
  (void)ev;
#endif // !defined(ASIO_NO_DEPRECATED)
}

#if !defined(ASIO_NO_DEPRECATED)
void io_pool_context::service::fork_service(io_context::fork_event)
{
}
#endif // !defined(ASIO_NO_DEPRECATED)

} // namespace asio

#include "asio/detail/pop_options.hpp"

#endif // ASIO_IMPL_IO_POOL_CONTEXT_IPP
