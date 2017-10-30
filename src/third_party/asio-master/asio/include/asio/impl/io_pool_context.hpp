//
// impl/io_context.hpp
// ~~~~~~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2017 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef ASIO_IMPL_IO_POOL_CONTEXT_HPP
#define ASIO_IMPL_IO_POOL_CONTEXT_HPP

#if defined(_MSC_VER) && (_MSC_VER >= 1200)
# pragma once
#endif // defined(_MSC_VER) && (_MSC_VER >= 1200)

#include "asio/detail/completion_handler.hpp"
#include "asio/detail/executor_op.hpp"
#include "asio/detail/fenced_block.hpp"
#include "asio/detail/handler_type_requirements.hpp"
#include "asio/detail/recycling_allocator.hpp"
#include "asio/detail/service_registry.hpp"
#include "asio/detail/throw_error.hpp"
#include "asio/detail/type_traits.hpp"

#include "asio/detail/push_options.hpp"

namespace asio {

template <typename Service>
inline Service& use_service(io_pool_context& ioc)
{
  // Check that Service meets the necessary type requirements.
  (void)static_cast<execution_context::service*>(static_cast<Service*>(0));
  (void)static_cast<const execution_context::id*>(&Service::id);

  return ioc.service_registry_->template use_service<Service>(ioc);
}

template <>
inline detail::io_pool_context_impl& use_service<detail::io_pool_context_impl>(
	io_pool_context& ioc)
{
  return ioc.impl_;
}

} // namespace asio

#include "asio/detail/pop_options.hpp"

#if defined(ASIO_HAS_IOCP)
# include "asio/detail/win_iocp_io_pool_context.hpp"
#else
# include "asio/detail/scheduler.hpp"
#endif

#include "asio/detail/push_options.hpp"

namespace asio {

inline io_pool_context::executor_type
io_pool_context::get_executor() ASIO_NOEXCEPT
{
  return executor_type(*this);
}

#if !defined(ASIO_NO_DEPRECATED)

template <typename CompletionHandler>
ASIO_INITFN_RESULT_TYPE(CompletionHandler, void ())
io_pool_context::dispatch(ASIO_MOVE_ARG(CompletionHandler) handler)
{
  // If you get an error on the following line it means that your handler does
  // not meet the documented type requirements for a CompletionHandler.
  ASIO_COMPLETION_HANDLER_CHECK(CompletionHandler, handler) type_check;

  async_completion<CompletionHandler, void ()> init(handler);

  if (impl_.can_dispatch())
  {
    detail::fenced_block b(detail::fenced_block::full);
    asio_handler_invoke_helpers::invoke(
        init.completion_handler, init.completion_handler);
  }
  else
  {
    // Allocate and construct an operation to wrap the handler.
    typedef detail::completion_handler<
      typename handler_type<CompletionHandler, void ()>::type> op;
    typename op::ptr p = { detail::addressof(init.completion_handler),
      op::ptr::allocate(init.completion_handler), 0 };
    p.p = new (p.v) op(init.completion_handler);

    ASIO_HANDLER_CREATION((*this, *p.p,
          "io_pool_context", this, 0, "dispatch"));

    impl_.do_dispatch(p.p);
    p.v = p.p = 0;
  }

  return init.result.get();
}

template <typename CompletionHandler>
ASIO_INITFN_RESULT_TYPE(CompletionHandler, void ())
io_pool_context::post(ASIO_MOVE_ARG(CompletionHandler) handler)
{
  // If you get an error on the following line it means that your handler does
  // not meet the documented type requirements for a CompletionHandler.
  ASIO_COMPLETION_HANDLER_CHECK(CompletionHandler, handler) type_check;

  async_completion<CompletionHandler, void ()> init(handler);

  bool is_continuation =
    asio_handler_cont_helpers::is_continuation(init.completion_handler);

  // Allocate and construct an operation to wrap the handler.
  typedef detail::completion_handler<
    typename handler_type<CompletionHandler, void ()>::type> op;
  typename op::ptr p = { detail::addressof(init.completion_handler),
      op::ptr::allocate(init.completion_handler), 0 };
  p.p = new (p.v) op(init.completion_handler);

  ASIO_HANDLER_CREATION((*this, *p.p,
        "io_pool_context", this, 0, "post"));

  impl_.post_immediate_completion(p.p, is_continuation);
  p.v = p.p = 0;

  return init.result.get();
}

template <typename Handler>
#if defined(GENERATING_DOCUMENTATION)
unspecified
#else
inline detail::wrapped_handler<io_context&, Handler>
#endif
io_pool_context::wrap(Handler handler)
{
  return detail::wrapped_handler<io_context&, Handler>(*this, handler);
}

#endif // !defined(ASIO_NO_DEPRECATED)

inline io_pool_context&
io_pool_context::executor_type::context() const ASIO_NOEXCEPT
{
  return io_context_;
}

inline void
io_pool_context::executor_type::on_work_started() const ASIO_NOEXCEPT
{
  io_context_.impl_.work_started();
}

inline void
io_pool_context::executor_type::on_work_finished() const ASIO_NOEXCEPT
{
  io_context_.impl_.work_finished();
}

template <typename Function, typename Allocator>
void io_pool_context::executor_type::dispatch(
    ASIO_MOVE_ARG(Function) f, const Allocator& a) const
{
  typedef typename decay<Function>::type function_type;

  // Invoke immediately if we are already inside the thread pool.
  if (io_context_.impl_.can_dispatch())
  {
    // Make a local, non-const copy of the function.
    function_type tmp(ASIO_MOVE_CAST(Function)(f));

    detail::fenced_block b(detail::fenced_block::full);
    asio_handler_invoke_helpers::invoke(tmp, tmp);
    return;
  }

  // Allocate and construct an operation to wrap the function.
  typedef detail::executor_op<function_type, Allocator, detail::operation> op;
  typename op::ptr p = { detail::addressof(a), op::ptr::allocate(a), 0 };
  p.p = new (p.v) op(ASIO_MOVE_CAST(Function)(f), a);

  ASIO_HANDLER_CREATION((this->context(), *p.p,
        "io_pool_context", &this->context(), 0, "post"));

  io_context_.impl_.post_immediate_completion(p.p, false);
  p.v = p.p = 0;
}

template <typename Function, typename Allocator>
void io_pool_context::executor_type::post(
    ASIO_MOVE_ARG(Function) f, const Allocator& a) const
{
  typedef typename decay<Function>::type function_type;

  // Allocate and construct an operation to wrap the function.
  typedef detail::executor_op<function_type, Allocator, detail::operation> op;
  typename op::ptr p = { detail::addressof(a), op::ptr::allocate(a), 0 };
  p.p = new (p.v) op(ASIO_MOVE_CAST(Function)(f), a);

  ASIO_HANDLER_CREATION((this->context(), *p.p,
        "io_pool_context", &this->context(), 0, "post"));

  io_context_.impl_.post_immediate_completion(p.p, false);
  p.v = p.p = 0;
}

template <typename Function, typename Allocator>
void io_pool_context::executor_type::defer(
    ASIO_MOVE_ARG(Function) f, const Allocator& a) const
{
  typedef typename decay<Function>::type function_type;

  // Allocate and construct an operation to wrap the function.
  typedef detail::executor_op<function_type, Allocator, detail::operation> op;
  typename op::ptr p = { detail::addressof(a), op::ptr::allocate(a), 0 };
  p.p = new (p.v) op(ASIO_MOVE_CAST(Function)(f), a);

  ASIO_HANDLER_CREATION((this->context(), *p.p,
        "io_pool_context", &this->context(), 0, "defer"));

  io_context_.impl_.post_immediate_completion(p.p, true);
  p.v = p.p = 0;
}

inline bool
io_pool_context::executor_type::running_in_this_thread() const ASIO_NOEXCEPT
{
  return io_context_.impl_.can_dispatch();
}

#if !defined(ASIO_NO_DEPRECATED)
inline io_pool_context::work::work(asio::io_pool_context& io_context)
  : io_context_impl_(io_context.impl_)
{
  io_context_impl_.work_started();
}

inline io_pool_context::work::work(const work& other)
  : io_context_impl_(other.io_context_impl_)
{
  io_context_impl_.work_started();
}

inline io_pool_context::work::~work()
{
  io_context_impl_.work_finished();
}

inline asio::io_pool_context& io_pool_context::work::get_io_context()
{
  return static_cast<asio::io_pool_context&>(io_context_impl_.context());
}

inline asio::io_pool_context& io_pool_context::work::get_io_service()
{
  return static_cast<asio::io_pool_context&>(io_context_impl_.context());
}
#endif // !defined(ASIO_NO_DEPRECATED)

inline asio::io_pool_context& io_pool_context::service::get_io_context()
{
  return static_cast<asio::io_pool_context&>(context());
}

#if !defined(ASIO_NO_DEPRECATED)
inline asio::io_pool_context& io_pool_context::service::get_io_service()
{
  return static_cast<asio::io_pool_context&>(context());
}
#endif // !defined(ASIO_NO_DEPRECATED)

} // namespace asio

#include "asio/detail/pop_options.hpp"

#endif // ASIO_IMPL_IO_POOL_CONTEXT_HPP
