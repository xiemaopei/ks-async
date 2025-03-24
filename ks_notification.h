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


class ks_notification {
public:
	ks_notification() {
		m_data_p = nullptr;
	}

	ks_notification(const ks_notification& r) {
		m_data_p = r.m_data_p;
		if (m_data_p != nullptr)
			++m_data_p->ref_count;
	}

	ks_notification& operator=(const ks_notification& r) {
		if (this != &r && m_data_p != r.m_data_p) {
			do_release_notification_data();
			m_data_p = r.m_data_p;
			if (m_data_p != nullptr)
				++m_data_p->ref_count;
		}
		return *this;
	}

	ks_notification(ks_notification&& r) noexcept {
		m_data_p = r.m_data_p;
		r.m_data_p = nullptr;
	}

	ks_notification& operator=(ks_notification&& r) noexcept {
		do_release_notification_data();
		m_data_p = r.m_data_p;
		r.m_data_p = nullptr;
		return *this;
	}

	~ks_notification() {
		do_release_notification_data();
	}

public:
	ks_notification& set_sender(const void* sender) {
		do_prepare_notification_data_cow();
		m_data_p->sender = sender;
		return *this;
	}

	ks_notification& set_notification_name(const char* notification_name) {
		do_prepare_notification_data_cow();
		m_data_p->notification_name = notification_name;
		return *this;
	}

	template <class DATA_TYPE, class X = DATA_TYPE>
	ks_notification& set_notification_data(const X& notification_data) {
		do_prepare_notification_data_cow();
		m_data_p->notification_data_any = ks_any::of<DATA_TYPE>(notification_data);
		return *this;
	}

	ks_notification& set_notification_context(const ks_async_context& notification_context) {
		do_prepare_notification_data_cow();
		m_data_p->notification_context = notification_context;
		return *this;
	}

public:
	const void* get_sender() const {
		return m_data_p != nullptr ? m_data_p->sender : nullptr;
	}

	const char* get_notification_name() const {
		return m_data_p != nullptr ? m_data_p->notification_name.c_str() : nullptr;
	}

	template <class DATA_TYPE>
	const DATA_TYPE& get_notification_data() const { 
		return m_data_p != nullptr ? m_data_p->notification_data_any.get<DATA_TYPE>() : ks_any().get<DATA_TYPE>();
	}

	ks_async_context get_notification_context() const { 
		return m_data_p != nullptr ? m_data_p->notification_context : ks_async_context{};
	}

private:
	struct __NOTIFICATION_DATA {
		const void* sender = nullptr;
		std::string notification_name{};
		ks_any notification_data_any{};
		ks_async_context notification_context = ks_async_context::__empty_inst();
		std::atomic<int> ref_count = { 1 };
	};

private:
	void do_prepare_notification_data_cow() {
		if (m_data_p == nullptr) {
			m_data_p = new __NOTIFICATION_DATA();
		}
		else if (m_data_p->ref_count >= 2) {
			__NOTIFICATION_DATA* origData = m_data_p;
			__NOTIFICATION_DATA* copyData = new __NOTIFICATION_DATA();
			copyData->sender = origData->sender;
			copyData->notification_name = origData->notification_name;
			copyData->notification_data_any = origData->notification_data_any;
			copyData->notification_context = origData->notification_context;

			do_release_notification_data();
			m_data_p = copyData;
		}
	}

	void do_release_notification_data() {
		if (m_data_p != nullptr) {
			if (--m_data_p->ref_count == 0) {
				delete m_data_p;
				m_data_p = nullptr;
			}
		}
	}

private:
	__NOTIFICATION_DATA* m_data_p = nullptr;
};
