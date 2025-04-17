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

#include "../ks_async_base.h"
#include "ks_raw_future.h"

__KS_ASYNC_RAW_BEGIN


template <class T>
class ks_raw_smart_ptr {
public:
	ks_raw_smart_ptr() : m_p(nullptr) {}
	explicit ks_raw_smart_ptr(T* p) : m_p(p) { ASSERT(m_p == nullptr || m_p->ref_count == 1); }

	ks_raw_smart_ptr(const ks_raw_smart_ptr& r) {
		m_p = r.m_p;
		if (m_p != nullptr) 
			++m_p->ref_count;
	}
	ks_raw_smart_ptr& operator=(const ks_raw_smart_ptr& r) {
		if (this != &r && m_p != r.m_p) {
			this->reset();
			m_p = r.m_p;
			if (m_p != nullptr)
				++m_p->ref_count;
		}
		return *this;
	}

	ks_raw_smart_ptr(ks_raw_smart_ptr&& r) noexcept {
		m_p = r.m_p;
		r.m_p = nullptr;
	}
	ks_raw_smart_ptr& operator=(ks_raw_smart_ptr&& r) noexcept {
		ASSERT(this != &r);
		this->reset();
		m_p = r.m_p;
		r.m_p = nullptr;
		return *this;
	}

	~ks_raw_smart_ptr() {
		this->reset(); 
	}

	template <class... ARGS>
	void create_instance(ARGS&&... args) {
		ASSERT(m_p == nullptr);
		if (m_p == nullptr) {
			m_p = new T(std::forward<ARGS>(args)...);
			ASSERT(m_p->ref_count == 1);
		}
	}

	void reset() { 
		if (m_p != nullptr) {
			if (--m_p->ref_count == 0) 
				delete m_p;
			m_p = nullptr;
		}
	}

	T* get() const { return m_p; }
	T* operator->() const { ASSERT(m_p != nullptr); return m_p; }

	bool operator==(const ks_raw_smart_ptr<T>& r) const { return m_p == r.m_p; }
	bool operator!=(const ks_raw_smart_ptr<T>& r) const { return m_p != r.m_p; }
	bool operator==(T* p) const { return m_p == p; }
	bool operator!=(T* p) const { return m_p != p; }

private:
	T* m_p;
};


class ks_raw_running_future_rtstt {
public:
	ks_raw_running_future_rtstt() {}
	~ks_raw_running_future_rtstt() { this->try_unapply(); }

	_DISABLE_COPY_CONSTRUCTOR(ks_raw_running_future_rtstt);

public:
	void apply(ks_raw_future* cur_future, ks_raw_future** tls_current_thread_running_future_addr) {
		if (m_applied_flag) {
			ASSERT(false);
			this->try_unapply();
		}

		ASSERT(cur_future != nullptr);
		ASSERT(tls_current_thread_running_future_addr != nullptr);

		m_applied_flag = true;
		m_tls_current_thread_running_future_addr = tls_current_thread_running_future_addr;
		m_cur_future = cur_future;
		m_current_thread_future_backup = *m_tls_current_thread_running_future_addr;
		*m_tls_current_thread_running_future_addr = cur_future;
	}

	void try_unapply() {
		if (!m_applied_flag)
			return;

		ASSERT(*m_tls_current_thread_running_future_addr == m_cur_future);

		m_applied_flag = false;
		*m_tls_current_thread_running_future_addr = m_current_thread_future_backup;
		m_tls_current_thread_running_future_addr = nullptr;
		m_cur_future = nullptr;
		m_current_thread_future_backup = nullptr;
	}

private:
	bool m_applied_flag = false;
	ks_raw_future** m_tls_current_thread_running_future_addr = nullptr;
	ks_raw_future* m_cur_future = nullptr;
	ks_raw_future* m_current_thread_future_backup = nullptr;
};


class ks_raw_living_context_rtstt {
public:
	ks_raw_living_context_rtstt() {}
	~ks_raw_living_context_rtstt() { this->try_unapply(); }

	_DISABLE_COPY_CONSTRUCTOR(ks_raw_living_context_rtstt);

public:
	void apply(const ks_async_context& cur_context) {
		if (m_applied_flag) {
			ASSERT(false);
			this->try_unapply();
		}

		m_applied_flag = true;

		m_cur_context = cur_context;
		m_cur_context_owner_locker = m_cur_context.__lock_owner_ptr();
		m_cur_context.__increment_pending_count();
	}

	void try_unapply() {
		if (!m_applied_flag)
			return;

		m_cur_context.__unlock_owner_ptr(m_cur_context_owner_locker);
		m_cur_context.__decrement_pending_count();

		m_applied_flag = false;
		m_cur_context = ks_async_context::__empty_inst();
		m_cur_context_owner_locker = {};
	}

	ks_any get_owner_locker() const {
		return m_cur_context_owner_locker;
	}

private:
	bool m_applied_flag = false;
	ks_async_context m_cur_context;
	ks_any m_cur_context_owner_locker;
};


__KS_ASYNC_RAW_END
