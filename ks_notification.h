/* Copyright 2024 The Kingsoft's ks-async Authors. All Rights Reserved.

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
#include "ks_async_context.h"
#include "ktl/ks_any.h"
#include <string>
#include <memory>


class ks_notification;
class ks_notification_builder;


class ks_notification {
public:
	ks_notification() = delete;
	ks_notification(const ks_notification&) = default;
	ks_notification& operator=(const ks_notification&) = default;
	ks_notification(ks_notification&&) noexcept = default;
	ks_notification& operator=(ks_notification&& r) noexcept = default; 

public:
	const void* get_sender() const {
		return m_data_ptr->sender;
	}

	const char* get_notification_name() const {
		return m_data_ptr->notification_name.c_str();
	}

	template <class DATA_TYPE>
	const DATA_TYPE& get_notification_data() const { 
		return m_data_ptr->notification_data_any.get<DATA_TYPE>();
	}

	ks_async_context get_notification_context() const { 
		return m_data_ptr->notification_context;
	}

private:
	struct __NOTIFICATION_DATA {
		const void* sender = nullptr;
		std::string notification_name;
		ks_any notification_data_any;
		ks_async_context notification_context;
	};

	explicit ks_notification(std::shared_ptr<__NOTIFICATION_DATA>&& data_ptr)
		: m_data_ptr(std::move(data_ptr)) {}

	friend class ks_notification_builder;

private:
	std::shared_ptr<__NOTIFICATION_DATA> m_data_ptr;
};


class ks_notification_builder {
public:
	ks_notification_builder() {}
	_DISABLE_COPY_CONSTRUCTOR(ks_notification_builder);

	ks_notification_builder& set_sender(const void* sender) {
		m_data.sender = sender;
		return *this;
	}

	ks_notification_builder& set_notification_name(const char* notification_name) {
		m_data.notification_name = notification_name;
		return *this;
	}

	template <class DATA_TYPE, class X = DATA_TYPE, class _ = std::enable_if_t<std::is_convertible_v<X, DATA_TYPE>>>
	ks_notification_builder& set_notification_data(X&& notification_data) {
		m_data.notification_data_any = ks_any::of<DATA_TYPE>(std::forward<X>(notification_data));
		return *this;
	}

	ks_notification_builder& set_notification_context(const ks_async_context& notification_context) {
		m_data.notification_context = notification_context;
		return *this;
	}

	ks_notification build() {
		std::shared_ptr<__NOTIFICATION_DATA> data_ptr = std::make_shared<__NOTIFICATION_DATA>(std::move(m_data));
		return ks_notification(std::move(data_ptr));
	}

private:
	using __NOTIFICATION_DATA = ks_notification::__NOTIFICATION_DATA;

	__NOTIFICATION_DATA m_data;
};
