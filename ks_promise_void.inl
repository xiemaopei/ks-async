﻿/* Copyright 2024 The Kingsoft's ks-async Authors. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#pragma once

template <>
class ks_promise<void> final {
public:
	ks_promise(nullptr_t) : m_nothing_promise(nullptr) {}
	ks_promise(const ks_promise&) = default;
	ks_promise& operator=(const ks_promise&) = default;
	ks_promise(ks_promise&&) noexcept = default;
	ks_promise& operator=(ks_promise&&) noexcept = default;

	using arg_type = void;
	using value_type = void;
	using this_promise_type = ks_promise<void>;

public:
	static ks_promise<void> create() {
		return ks_promise<nothing_t>::create();
	}

public:
	bool is_valid() const {
		return m_nothing_promise.is_valid();
	}

	ks_future<void> get_future() const {
		return m_nothing_promise.get_future().cast<void>();
	}

	void resolve(nothing_t _ = nothing) const {
		m_nothing_promise.resolve(nothing);
	}

	void reject(const ks_error& error) const {
		m_nothing_promise.reject(error);
	}

	void try_complete(const ks_result<void>& result) const {
		if (result.is_value())
			this->resolve();
		else if (result.is_error())
			this->reject(result.to_error());
		else
			ASSERT(false);
	}

	void try_complete(const ks_result<nothing_t>& result) const {
		return this->try_complete(ks_result<void>::__from_other(result));
	}

private:
	using ks_raw_future = __ks_async_raw::ks_raw_future;
	using ks_raw_future_ptr = __ks_async_raw::ks_raw_future_ptr;
	using ks_raw_promise = __ks_async_raw::ks_raw_promise;
	using ks_raw_promise_ptr = __ks_async_raw::ks_raw_promise_ptr;

	using ks_raw_result = __ks_async_raw::ks_raw_result;
	using ks_raw_value = __ks_async_raw::ks_raw_value;

	ks_promise(ks_promise<nothing_t>&& nothing_promise) noexcept : m_nothing_promise(std::move(nothing_promise)) {}

	static ks_promise<void> __from_raw(const ks_raw_promise_ptr& raw_promise) { return ks_promise<nothing_t>::__from_raw(raw_promise); }
	static ks_promise<void> __from_raw(ks_raw_promise_ptr&& raw_promise) { return ks_promise<nothing_t>::__from_raw(std::move(raw_promise)); }
	const ks_raw_promise_ptr& __get_raw() const { return m_nothing_promise.__get_raw(); }

	template <class T2> friend class ks_future;
	template <class T2> friend class ks_promise;
	friend class ks_future_util;

private:
	ks_promise<nothing_t> m_nothing_promise;
};