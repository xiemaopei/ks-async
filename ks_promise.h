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

#include "ks_async_base.h"
#include "ks_future.h"
#include "ks_result.h"
#include "ks-async-raw/ks_raw_future.h"
#include "ks-async-raw/ks_raw_promise.h"


template <class T>
class ks_promise final {
public:
	ks_promise(nullptr_t) : m_raw_promise(nullptr) {}
	ks_promise(const ks_promise&) = default;
	ks_promise& operator=(const ks_promise&) = default;
	ks_promise(ks_promise&&) noexcept = default;
	ks_promise& operator=(ks_promise&&) noexcept = default;

	using arg_type = T;
	using value_type = T;
	using this_promise_type = ks_promise<T>;

public:
	static ks_promise<T> create() {
		ks_apartment* apartment_hint = ks_apartment::default_mta();
		ks_raw_promise_ptr raw_promise = ks_raw_promise::create(apartment_hint);
		return ks_promise<T>::__from_raw(raw_promise);
	}

public:
	bool is_valid() const {
		return m_raw_promise != nullptr;
	}

	ks_future<T> get_future() const {
		ASSERT(this->is_valid());
		return ks_future<T>::__from_raw(m_raw_promise->get_future());
	}

	void resolve(const T& value) const {
		ASSERT(this->is_valid());
		m_raw_promise->resolve(ks_raw_value::of(value));
	}

	void resolve(T&& value) const {
		ASSERT(this->is_valid());
		m_raw_promise->resolve(ks_raw_value::of(std::move(value)));
	}

	void reject(const ks_error& error) const {
		ASSERT(this->is_valid());
		m_raw_promise->reject(error);
	}

	void try_complete(const ks_result<T>& result) const {
		if (result.is_value())
			this->resolve(result.to_value());
		else if (result.is_error())
			this->reject(result.to_error());
		else
			ASSERT(false);
	}

private:
	using ks_raw_future = __ks_async_raw::ks_raw_future;
	using ks_raw_future_ptr = __ks_async_raw::ks_raw_future_ptr;
	using ks_raw_promise = __ks_async_raw::ks_raw_promise;
	using ks_raw_promise_ptr = __ks_async_raw::ks_raw_promise_ptr;

	using ks_raw_result = __ks_async_raw::ks_raw_result;
	using ks_raw_value = __ks_async_raw::ks_raw_value;

	explicit ks_promise(const ks_raw_promise_ptr& raw_promise, int) : m_raw_promise(raw_promise) {}
	explicit ks_promise(ks_raw_promise_ptr&& raw_promise, int) : m_raw_promise(std::move(raw_promise)) {}

	static ks_promise<T> __from_raw(const ks_raw_promise_ptr& raw_promise) { return ks_promise<T>(raw_promise, 0); }
	static ks_promise<T> __from_raw(ks_raw_promise_ptr&& raw_promise) { return ks_promise<T>(std::move(raw_promise), 0); }
	const ks_raw_promise_ptr& __get_raw() const { return m_raw_promise; }

	template <class T2> friend class ks_future;
	template <class T2> friend class ks_promise;
	friend class ks_future_util;

private:
	ks_raw_promise_ptr m_raw_promise;
};


#include "ks_promise_void.inl"