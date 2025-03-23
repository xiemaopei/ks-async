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
class ks_result<void> final {
public:
	ks_result(const nothing_t&) : m_nothing_result(nothing) {}
	ks_result(nothing_t&&) : m_nothing_result(nothing) {}

	ks_result(const ks_error& error) : m_nothing_result(error) {}
	ks_result(ks_error&& error) : m_nothing_result(std::move(error)) {}

	ks_result(const ks_result&) = default;
	ks_result(ks_result&&) noexcept = default;
	ks_result& operator=(const ks_result&) = default;
	ks_result& operator=(ks_result&&) noexcept = default;

	static ks_result __bare() { return ks_result(__raw_ctor::v); }

	using value_type = void;
	using this_result_type = ks_result<void>;

public:
	bool is_completed() const { return m_nothing_result.is_completed(); }
	bool is_value() const { return m_nothing_result.is_value(); }
	bool is_error() const { return m_nothing_result.is_error(); }

	nothing_t to_value() const noexcept(false) { return (m_nothing_result.to_value(), nothing); }
	ks_error to_error() const noexcept(false) { return m_nothing_result.to_error(); }

	template <class R>
	ks_result<R> cast() const {
		return m_nothing_result.template cast<R>();
	}

	template <class R, class FN = std::function<R()>>
	ks_result<R> map(FN&& fn) const {
		return m_nothing_result.template map<R>([fn = std::forward<FN>(fn)](nothing_t) { return fn(); });
	}

	template <class R, class X = R>
	ks_result<R> map_value(X&& x) const {
		return m_nothing_result.template map_value<R>(std::forward<X>(x));
	}

private:
	using __cast_mode_t = ks_result<nothing_t>::__cast_mode_t;

	template <class R>
	static constexpr __cast_mode_t __determine_cast_mode() {
		return  ks_result<nothing_t>::__determine_cast_mode();
	}

private:
	using ks_raw_result = __ks_async_raw::ks_raw_result;
	using ks_raw_value = __ks_async_raw::ks_raw_value;

	enum class __raw_ctor { v };
	explicit ks_result(__raw_ctor) : m_nothing_result(ks_result<nothing_t>::__raw_ctor::v) {}

	explicit ks_result(const ks_result<nothing_t>& other) : m_nothing_result(other) {}
	explicit ks_result(ks_result<nothing_t>&& other) noexcept : m_nothing_result(std::move(other)) {}

	static ks_result<void> __from_other(const ks_result<nothing_t>& other) { return ks_result<void>(other); }
	static ks_result<void> __from_other(ks_result<nothing_t>&& other) { return ks_result<void>(std::move(other)); }

	static ks_result<void> __from_raw(const ks_raw_result& raw_result) {
		if (raw_result.is_value())
			return ks_result<void>(nothing);
		else if (raw_result.is_error())
			return ks_result<void>(raw_result.to_error());
		else
			return ks_result<void>(ks_result<void>::__raw_ctor::v);
	}

	static ks_result<void> __from_raw(ks_raw_result&& raw_result) {
		return ks_result<void>::__from_raw(raw_result);
	}

	ks_raw_result __get_raw() const {
		return m_nothing_result.__get_raw();
	}

	template <class T2> friend class ks_result;
	template <class T2> friend class ks_future;
	template <class T2> friend class ks_promise;
	friend class ks_future_util;
	friend class ks_async_flow;

private:
	ks_result<nothing_t> m_nothing_result;
};
