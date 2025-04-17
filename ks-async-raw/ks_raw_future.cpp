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

#include "ks_raw_future.h"
#include "ks_raw_promise.h"
#include "ks_raw_internal_helper.h"
#include "../ktl/ks_concurrency.h"
#include "../ktl/ks_deferrer.h"
#include <algorithm>
#include <set>

void __forcelink_to_ks_raw_future_cpp() {}

#if (!__KS_APARTMENT_ATFORK_ENABLED)
	using __native_pid_t = int;
	static inline __native_pid_t __native_get_current_pid() { return -1; }  //pseudo
	static constexpr __native_pid_t __native_pid_none = 0;
#elif defined(_WIN32)
	#include <Windows.h>
	#include <processthreadsapi.h>
	using __native_pid_t = DWORD;
	static inline __native_pid_t __native_get_current_pid() { return ::GetCurrentProcessId(); }
	static constexpr __native_pid_t __native_pid_none = 0;
#elif defined(__APPLE__)
	#include <sys/proc.h>
	using __native_pid_t = int;
	static inline __native_pid_t __native_get_current_pid() { return proc_selfpid(); }
	static constexpr __native_pid_t __native_pid_none = 0;
#else
	#include <unistd.h>
	using __native_pid_t = pid_t;
	static inline __native_pid_t __native_get_current_pid() { return getpid(); }
	static constexpr __native_pid_t __native_pid_none = 0;
#endif


__KS_ASYNC_RAW_BEGIN

static thread_local ks_raw_future* tls_current_thread_running_future = nullptr;


enum class ks_raw_future_mode {
	DX, PROMISE,  //promise
	TASK, TASK_DELAYED,  //task
	THEN, TRAP, TRANSFORM, FORWARD,  //pipe
	FLATTEN_THEN, FLATTEN_TRAP, FLATTEN_TRANSFORM,  //flatten
	ALL, ALL_COMPLETED, ANY, //aggr
};

class ks_raw_future_baseimp : public ks_raw_future, public std::enable_shared_from_this<ks_raw_future> {
protected:
	explicit ks_raw_future_baseimp(ks_raw_future_mode mode, bool cancelable, ks_apartment* spec_apartment, const ks_async_context& living_context)
		: m_mode(mode), m_cancelable(cancelable), m_spec_apartment(spec_apartment) {
		m_intermediate_data_ptr = std::make_shared<__INTERMEDIATE_DATA>();
		m_intermediate_data_ptr->m_living_context = living_context;
		m_intermediate_data_ptr->m_create_time = std::chrono::steady_clock::now();
	}
	explicit ks_raw_future_baseimp(ks_raw_future_mode mode, bool cancelable, ks_apartment* spec_apartment, const ks_raw_result& immediate_result)
		: m_mode(mode), m_cancelable(cancelable), m_spec_apartment(spec_apartment) {
		ASSERT(immediate_result.is_completed());
		m_completed_result = immediate_result;
		m_completed_prefer_apartment = this->do_determine_prefer_apartment(nullptr);
	}

	_DISABLE_COPY_CONSTRUCTOR(ks_raw_future_baseimp);

public:
	virtual ks_raw_future_ptr then(std::function<ks_raw_result(const ks_raw_value&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;
	virtual ks_raw_future_ptr trap(std::function<ks_raw_result(const ks_error&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;
	virtual ks_raw_future_ptr transform(std::function<ks_raw_result(const ks_raw_result&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;

	virtual ks_raw_future_ptr flat_then(std::function<ks_raw_future_ptr(const ks_raw_value&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;
	virtual ks_raw_future_ptr flat_trap(std::function<ks_raw_future_ptr(const ks_error&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;
	virtual ks_raw_future_ptr flat_transform(std::function<ks_raw_future_ptr(const ks_raw_result&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;

	virtual ks_raw_future_ptr on_success(std::function<void(const ks_raw_value&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;
	virtual ks_raw_future_ptr on_failure(std::function<void(const ks_error&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;
	virtual ks_raw_future_ptr on_completion(std::function<void(const ks_raw_result&)>&& fn, const ks_async_context& context, ks_apartment* apartment) override;

	virtual ks_raw_future_ptr noop(ks_apartment* apartment) override;

public:
	virtual bool is_completed() override {
		std::unique_lock<ks_mutex> lock(m_mutex);
		return m_completed_result.is_completed();
	}

	virtual ks_raw_result peek_result() override {
		std::unique_lock<ks_mutex> lock(m_mutex);
		return m_completed_result;
	}

protected:
	virtual bool do_check_cancel() override {
		std::unique_lock<ks_mutex> lock(m_mutex, std::defer_lock);
		return this->do_check_cancel_locking(lock);
	}

	virtual ks_error do_acquire_cancel_error(const ks_error& def_error) override {
		std::unique_lock<ks_mutex> lock(m_mutex, std::defer_lock);
		return this->do_acquire_cancel_error_locking(def_error, lock);
	}

	bool do_check_cancel_locking(std::unique_lock<ks_mutex>& lock) {
		if (!m_completed_result.is_completed()) {
			ASSERT(m_intermediate_data_ptr != nullptr);

			if (m_intermediate_data_ptr->m_cancel_error.get_code() != 0)
				return true;

			if (m_intermediate_data_ptr->m_living_context.__is_controller_present()) {
				if (m_intermediate_data_ptr->m_living_context.__check_cancel_all_ctrl() || m_intermediate_data_ptr->m_living_context.__check_owner_expired())
					return true;
			}

			if (m_intermediate_data_ptr->m_timeout_schedule_id != 0) {
				if (m_intermediate_data_ptr->m_timeout_schedule_id != 0 && (m_intermediate_data_ptr->m_timeout_time <= std::chrono::steady_clock::now()))
					return true;
			}
		}
		else if (m_completed_result.is_error()) {
			return true;
		}

		return false;
	}

	ks_error do_acquire_cancel_error_locking(const ks_error& def_error, std::unique_lock<ks_mutex>& lock) {
		if (!m_completed_result.is_completed()) {
			ASSERT(m_intermediate_data_ptr != nullptr);

			if (m_intermediate_data_ptr->m_cancel_error.get_code() != 0)
				return m_intermediate_data_ptr->m_cancel_error;

			if (m_intermediate_data_ptr->m_living_context.__is_controller_present()) {
				if (m_intermediate_data_ptr->m_living_context.__check_cancel_all_ctrl() || m_intermediate_data_ptr->m_living_context.__check_owner_expired())
					return ks_error::cancelled_error();
			}

			if (m_intermediate_data_ptr->m_timeout_schedule_id != 0) {
				if (m_intermediate_data_ptr->m_timeout_schedule_id != 0 && (m_intermediate_data_ptr->m_timeout_time <= std::chrono::steady_clock::now()))
					return ks_error::timeout_error();
			}
		}
		else if (m_completed_result.is_error()) {
			return m_completed_result.to_error();
		}

		return def_error;
	}

	virtual bool do_wait() override {
		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return true;

		ASSERT(m_intermediate_data_ptr != nullptr);
		const auto intermediate_data_ptr = m_intermediate_data_ptr;

		ks_apartment* cur_apartment = ks_apartment::current_thread_apartment();
		ASSERT(cur_apartment == nullptr || (cur_apartment->features() & ks_apartment::nested_pump_enabled_future) != 0);
		if (cur_apartment != nullptr && (cur_apartment->features() & ks_apartment::nested_pump_enabled_future) != 0) {
			intermediate_data_ptr->m_waiting_for_me_apartment_set.insert(cur_apartment); //若嵌套loop会遭遇相同项，但不必重复记录，因为至多仅顶层可能会卡在真cv.wait调用处

			lock.unlock();
			bool was_satisfied = cur_apartment->__do_run_nested_pump_loop_for_extern_waiting(
				this,
				[this, this_shared = this->shared_from_this()]() -> bool { return m_completed_result.is_completed(); });
			ASSERT(was_satisfied ? m_completed_result.is_completed() : true);

			lock.lock();
			intermediate_data_ptr->m_waiting_for_me_apartment_set.erase(cur_apartment); //若退嵌套loop则会遭遇缺失项，这是正常的

			if (!m_completed_result.is_completed()) {
				//若nested_pump_loop已退出，但又非completed，理论上是在atforking了，那么我们只能立即结束future的wait，
				//又因wait结束，那么也只好将当前future标记为失败了，因为后续此future理所当然会被认为是completed状态了，
				//但实际上我们期望不要发生此情形，即在有future在wait时不期望进行进程fork，因此这里ASSERT(false)。
				ASSERT(false);
				this->do_complete_locked(ks_error::interrupted_error(), cur_apartment, false, lock, false);
				return false;
			}

			return true;
		}
		else {
			if (intermediate_data_ptr->m_completion_cv_waiting_rc == 0) {
				ASSERT(intermediate_data_ptr->m_completion_cv_belong_pid == __native_pid_none);
				intermediate_data_ptr->m_completion_cv_belong_pid = __native_get_current_pid();
			}
			else if (intermediate_data_ptr->m_completion_cv_belong_pid != __native_get_current_pid()) {
				ASSERT(intermediate_data_ptr->m_completion_cv_belong_pid != __native_pid_none);
				intermediate_data_ptr->m_completion_cv_belong_pid = __native_get_current_pid();
				::new (&intermediate_data_ptr->m_completion_cv) ks_condition_variable(); //重建cv，在子进程中
			}

			++intermediate_data_ptr->m_completion_cv_waiting_rc;

			while (!m_completed_result.is_completed()) {
				intermediate_data_ptr->m_completion_cv.wait(lock);
			}

			if (--intermediate_data_ptr->m_completion_cv_waiting_rc == 0) {
				if (intermediate_data_ptr->m_completion_cv_belong_pid != __native_get_current_pid())
					::new (&intermediate_data_ptr->m_completion_cv) ks_condition_variable(); //重建cv，马上就要析构了
			}

			return true;
		}
	}

	virtual ks_apartment* get_spec_apartment() override {
		return m_spec_apartment;
	}

protected:
	virtual void do_add_next(const ks_raw_future_ptr& next_future) override {
		std::unique_lock<ks_mutex> lock(m_mutex);
		return this->do_add_next_locked(next_future, lock);
	}

	virtual void do_add_next_multi(const std::vector<ks_raw_future_ptr>& next_futures) override {
		if (!next_futures.empty())
			return;

		std::unique_lock<ks_mutex> lock(m_mutex);
		return this->do_add_next_multi_locked(next_futures, lock);
	}

	virtual void do_complete(const ks_raw_result& result, ks_apartment* prefer_apartment, bool from_internal) override {
		std::unique_lock<ks_mutex> lock(m_mutex);
		return this->do_complete_locked(result, prefer_apartment, from_internal, lock, false);
	}

	void do_add_next_locked(const ks_raw_future_ptr& next_future, std::unique_lock<ks_mutex>& lock) {
		if (!m_completed_result.is_completed()) {
			ASSERT(m_intermediate_data_ptr != nullptr);

			if (m_intermediate_data_ptr->m_next_future_0 == nullptr)
				m_intermediate_data_ptr->m_next_future_0 = next_future;
			else
				m_intermediate_data_ptr->m_next_future_more.push_back(next_future);
		}
		else {
			ks_raw_result result = m_completed_result;
			ks_apartment* prefer_apartment = m_completed_prefer_apartment;

			uint64_t act_schedule_id = prefer_apartment->schedule([this, this_shared = this->shared_from_this(), next_future, result, prefer_apartment]() {
				next_future->on_feeded_by_prev(result, this, prefer_apartment);
			}, 0);

			if (act_schedule_id == 0) {
				lock.unlock();
				next_future->do_complete(ks_error::terminated_error(), prefer_apartment, false);
				lock.lock();
			}
		}
	}

	void do_add_next_multi_locked(const std::vector<ks_raw_future_ptr>& next_futures, std::unique_lock<ks_mutex>& lock) {
		if (next_futures.empty())
			return;

		if (!m_completed_result.is_completed()) {
			ASSERT(m_intermediate_data_ptr != nullptr);

			auto next_future_it = next_futures.cbegin();
			if (m_intermediate_data_ptr->m_next_future_0 == nullptr)
				m_intermediate_data_ptr->m_next_future_0 = *next_future_it++;
			if (next_future_it != next_futures.cend()) {
				m_intermediate_data_ptr->m_next_future_more.insert(
					m_intermediate_data_ptr->m_next_future_more.end(),
					next_future_it, next_futures.cend());
			}
		}
		else {
			ks_raw_result result = m_completed_result;
			ks_apartment* prefer_apartment = m_completed_prefer_apartment;

			uint64_t act_schedule_id = prefer_apartment->schedule([this, this_shared = this->shared_from_this(), next_futures, result, prefer_apartment]() {
				for (auto& next_future : next_futures)
					next_future->on_feeded_by_prev(result, this, prefer_apartment);
			}, 0);

			if (act_schedule_id == 0) {
				lock.unlock();
				for (auto& next_future : next_futures)
					next_future->do_complete(ks_error::terminated_error(), prefer_apartment, false);
				lock.lock();
			}
		}
	}

	void do_complete_locked(const ks_raw_result& result, ks_apartment* prefer_apartment, bool from_internal, std::unique_lock<ks_mutex>& lock, bool must_keep_locked) {
		ASSERT(!must_keep_locked);
		ASSERT(result.is_completed());

		if (m_completed_result.is_completed()) 
			return; //repeat complete?

		if (prefer_apartment == nullptr)
			prefer_apartment = this->do_determine_prefer_apartment(nullptr);

		ASSERT(m_intermediate_data_ptr != nullptr);

		m_completed_result = result.require_completed_or_error();
		m_completed_prefer_apartment = prefer_apartment;

		m_intermediate_data_ptr->m_pending_flag_v = false;

		if (m_intermediate_data_ptr->m_completion_cv_waiting_rc != 0 && m_intermediate_data_ptr->m_completion_cv_belong_pid == __native_get_current_pid()) {
			m_intermediate_data_ptr->m_completion_cv.notify_all(); //子进程不要notify，会有几率卡死！（若子进程内执行wait，会更新子进程内记录的belong_pid的）
		}

		for (ks_apartment* apartment : m_intermediate_data_ptr->m_waiting_for_me_apartment_set) {
			apartment->__do_notify_nested_pump_loop_for_extern_waiting(this);
		}

		if (m_intermediate_data_ptr->m_timeout_schedule_id != 0) {
			uint64_t timeout_schedule_id = m_intermediate_data_ptr->m_timeout_schedule_id;
			m_intermediate_data_ptr->m_timeout_schedule_id = 0;
			m_intermediate_data_ptr->m_timeout_time = {};

			ks_apartment* timeout_apartment = this->do_determine_timeout_apartment();
			timeout_apartment->try_unschedule(timeout_schedule_id);
		}

		this->do_reset_extra_data_locked(lock);

		m_intermediate_data_ptr->m_living_context = ks_async_context::__empty_inst(); //clear

		if (m_intermediate_data_ptr->m_next_future_0 != nullptr || !m_intermediate_data_ptr->m_next_future_more.empty()) {
			ks_raw_future_ptr t_next_future_0;
			std::vector<ks_raw_future_ptr> t_next_future_more;
			t_next_future_0.swap(m_intermediate_data_ptr->m_next_future_0);
			t_next_future_more.swap(m_intermediate_data_ptr->m_next_future_more);
			m_intermediate_data_ptr->m_next_future_0 = nullptr;
			m_intermediate_data_ptr->m_next_future_more.clear();

			if (from_internal) {
				ASSERT(lock.owns_lock());
				lock.unlock(); //按说内部流程无需解锁（除非外部不合理乱用0x10000优先级）
				if (t_next_future_0 != nullptr)
					t_next_future_0->on_feeded_by_prev(result, this, prefer_apartment);
				for (auto& next_future : t_next_future_more)
					next_future->on_feeded_by_prev(result, this, prefer_apartment);
				if (must_keep_locked)
					lock.lock();
			}
			else {
				uint64_t act_schedule_id = prefer_apartment->schedule(
					[this, this_shared = this->shared_from_this(), 
					t_next_future_0, t_next_future_more,  //因为失败时还需要处理，所以不可以右值引用传递
					//t_next_future_0 = std::move(t_next_future_0), t_next_future_more = std::move(t_next_future_more),
					result = m_completed_result, prefer_apartment]() {
						if (t_next_future_0 != nullptr)
							t_next_future_0->on_feeded_by_prev(result, this, prefer_apartment);
						for (auto& next_future : t_next_future_more)
							next_future->on_feeded_by_prev(result, this, prefer_apartment);
				}, 0);

				if (act_schedule_id == 0) {
					ASSERT(lock.owns_lock());
					lock.unlock();
					if (t_next_future_0 != nullptr)
						t_next_future_0->on_feeded_by_prev(ks_error::terminated_error(), this, prefer_apartment);
					for (auto& next_future : t_next_future_more)
						next_future->on_feeded_by_prev(ks_error::terminated_error(), this, prefer_apartment);
					if (must_keep_locked)
						lock.lock();
				}
			}
		}

		//completed后清除
		m_intermediate_data_ptr.reset();
	}

	virtual void do_reset_extra_data_locked(std::unique_lock<ks_mutex>& lock) {}

	virtual void do_set_timeout(int64_t timeout, const ks_error& error, bool backtrack) override {
		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return;

		ASSERT(m_intermediate_data_ptr != nullptr);

		if (m_intermediate_data_ptr->m_timeout_schedule_id != 0) {
			ks_apartment* timeout_apartment = this->do_determine_timeout_apartment();
			timeout_apartment->try_unschedule(m_intermediate_data_ptr->m_timeout_schedule_id);
			m_intermediate_data_ptr->m_timeout_schedule_id = 0;
			m_intermediate_data_ptr->m_timeout_time = {};
		}

		if (timeout <= 0) 
			return; //infinity

		//not infinity
		m_intermediate_data_ptr->m_timeout_time = m_intermediate_data_ptr->m_create_time + std::chrono::milliseconds(timeout);

		const std::chrono::steady_clock::time_point now_time = std::chrono::steady_clock::now();
		const int64_t timeout_remain_ms = std::chrono::duration_cast<std::chrono::milliseconds>(m_intermediate_data_ptr->m_timeout_time - now_time).count();
		if (timeout_remain_ms <= 0) {
			if (!m_completed_result.is_completed()) {
				lock.unlock();
				this->do_try_cancel(error, backtrack);
			}
		}
		else {
			//schedule timeout
			ks_apartment* timeout_apartment = this->do_determine_timeout_apartment();
			m_intermediate_data_ptr->m_timeout_schedule_id = timeout_apartment->schedule_delayed(
				[this, this_shared = this->shared_from_this(), schedule_id = m_intermediate_data_ptr->m_timeout_schedule_id, error, backtrack]() -> void {
				std::unique_lock<ks_mutex> lock2(m_mutex);
				if (m_completed_result.is_completed())
					return;

				ASSERT(m_intermediate_data_ptr != nullptr);
				if (schedule_id != m_intermediate_data_ptr->m_timeout_schedule_id)
					return;

				m_intermediate_data_ptr->m_timeout_schedule_id = 0; //reset
				if (!m_intermediate_data_ptr->m_pending_flag_v || m_completed_result.is_completed())
					return;

				lock2.unlock();
				this->do_try_cancel(error, backtrack); //will become timeout
			}, 0, timeout_remain_ms);
			if (m_intermediate_data_ptr->m_timeout_schedule_id == 0) {
				ASSERT(false);
				return;
			}
		}
	}

	virtual void do_try_cancel(const ks_error& error, bool backtrack) override = 0;

protected:
	ks_apartment* do_determine_prefer_apartment(ks_apartment* advice_apartment) const {
		ks_apartment* prefer_apartment = m_spec_apartment != nullptr ? m_spec_apartment : advice_apartment;
		if (prefer_apartment == nullptr) 
			prefer_apartment = ks_apartment::current_thread_apartment_or_default_mta();
		return prefer_apartment;
	}

	ks_apartment* do_determine_timeout_apartment() const {
		return ks_apartment::default_mta();
	}

protected:
	ks_mutex m_mutex;

	ks_raw_future_mode m_mode;       //const-like
	bool m_cancelable;               //const-like
	ks_apartment* m_spec_apartment;  //const-like

	ks_raw_result m_completed_result{};
	ks_apartment* m_completed_prefer_apartment = nullptr;

	struct __INTERMEDIATE_DATA {
		ks_async_context m_living_context;                    //const-like
		std::chrono::steady_clock::time_point m_create_time;  //const-like

		ks_raw_future_ptr m_next_future_0;
		std::vector<ks_raw_future_ptr> m_next_future_more;

		std::chrono::steady_clock::time_point m_timeout_time = {};
		uint64_t m_timeout_schedule_id = 0;

		//fork子进程中操作cv有几率死锁，故子进程中不要使用它！
		//记录cv所属pid，保证进程内操作cv的一致性
		//必要时会重建cv，避免子进程卡死（只是尽量容错而已，并不绝对安全，尤其是逻辑上的死等）
		ks_condition_variable m_completion_cv{};
		__native_pid_t m_completion_cv_belong_pid = __native_pid_none;
		int m_completion_cv_waiting_rc = 0;

		std::set<ks_apartment*> m_waiting_for_me_apartment_set{};

		ks_error m_cancel_error{};
		volatile bool m_pending_flag_v = true;
	};

	std::shared_ptr<__INTERMEDIATE_DATA> m_intermediate_data_ptr; //completed后被清除

	friend class ks_raw_future;
};


class ks_raw_promise_future final : public ks_raw_future_baseimp, public ks_raw_promise {
public:
	//注：默认apartment原设计使用current_thread_apartment，现已改为使用default_mta
	explicit ks_raw_promise_future(ks_raw_future_mode mode, ks_apartment* spec_apartment, nullptr_t)
		: ks_raw_future_baseimp(mode, true, spec_apartment != nullptr ? spec_apartment : ks_apartment::default_mta(), ks_async_context::__empty_inst()) {}
	explicit ks_raw_promise_future(ks_raw_future_mode mode, ks_apartment* spec_apartment, const ks_raw_result& immediate_result)
		: ks_raw_future_baseimp(mode, true, spec_apartment != nullptr ? spec_apartment : ks_apartment::default_mta(), immediate_result) {}

	_DISABLE_COPY_CONSTRUCTOR(ks_raw_promise_future);

public: //override ks_raw_promise's methods
	virtual ks_raw_future_ptr get_future() override {
		return this->shared_from_this();
	}

	virtual void resolve(const ks_raw_value& value) override {
		this->do_complete(value, nullptr, false);
	}

	virtual void reject(const ks_error& error) override {
		this->do_complete(error, nullptr, false);
	}

	virtual void try_settle(const ks_raw_result& result) override {
		ASSERT(result.is_completed());
		this->do_complete(result.is_completed() ? result : ks_raw_result(ks_error::unexpected_error()), nullptr, false);
	}


protected:
	virtual void on_feeded_by_prev(const ks_raw_result& prev_result, ks_raw_future* prev_future, ks_apartment* prev_advice_apartment) override {
		//ks_raw_promise_future的此方法不应被调用，而是应直接do_complete
		ASSERT(false);
	}

	virtual void do_try_cancel(const ks_error& error, bool backtrack) override {
		ASSERT(error.get_code() != 0);

		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return;

		ASSERT(m_intermediate_data_ptr != nullptr);

		if (m_cancelable) {
			m_intermediate_data_ptr->m_cancel_error = error;
			this->do_complete_locked(error, nullptr, false, lock, false);
		}
	}

	virtual bool is_with_upstream_future() override {
		return false;
	}
};


class ks_raw_task_future final : public ks_raw_future_baseimp {
public:
	explicit ks_raw_task_future(ks_raw_future_mode mode, ks_apartment* spec_apartment, std::function<ks_raw_result()>&& task_fn, const ks_async_context& living_context, int64_t delay)
		: ks_raw_future_baseimp(mode, true, spec_apartment != nullptr ? spec_apartment : ks_apartment::default_mta(), living_context)
		, m_delay(delay) {
		m_extra_intermediate_data_ptr = std::make_shared<__EXTRA_INTERMEDIATE_DATA>();
		m_extra_intermediate_data_ptr->m_task_fn = std::move(task_fn);
	}

	_DISABLE_COPY_CONSTRUCTOR(ks_raw_task_future);

	void submit() {
		std::unique_lock<ks_mutex> lock(m_mutex);
		ASSERT(!m_completed_result.is_completed());
		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		ks_apartment* prefer_apartment = this->do_determine_prefer_apartment(nullptr);
		int priority = m_intermediate_data_ptr->m_living_context.__get_priority();
		bool could_run_locally = (priority >= 0x10000) && (m_spec_apartment == nullptr || m_spec_apartment == prefer_apartment);

		//pending_schedule_fn不对context进行捕获。
		//这样做的意图是：对于delayed任务，当try_cancel时，即使apartment::try_unschedule失败，也不影响context的及时释放。
		std::function<void()> pending_schedule_fn = [this, this_shared = this->shared_from_this(), prefer_apartment, context = m_intermediate_data_ptr->m_living_context]() mutable -> void {
			std::unique_lock<ks_mutex> lock2(m_mutex);
			if (m_completed_result.is_completed())
				return; //pre-check cancelled

			ASSERT(m_intermediate_data_ptr != nullptr);
			ASSERT(m_extra_intermediate_data_ptr != nullptr);

			m_intermediate_data_ptr->m_pending_flag_v = false;
			m_extra_intermediate_data_ptr->m_pending_schedule_id = 0; //这个变量第一时间被清0

			ks_raw_running_future_rtstt running_future_rtstt;
			ks_raw_living_context_rtstt living_context_rtstt;
			running_future_rtstt.apply(this, &tls_current_thread_running_future);
			living_context_rtstt.apply(context);

			ks_raw_result result;
			try {
				if (this->do_check_cancel_locking(lock2))
					result = this->do_acquire_cancel_error_locking(ks_error::cancelled_error(), lock2);
				else {
					std::function<ks_raw_result()> task_fn = std::move(m_extra_intermediate_data_ptr->m_task_fn);
					lock2.unlock();
					result = task_fn().require_completed_or_error();
					lock2.lock();
					task_fn = {};
				}
			}
			catch (ks_error error) {
				result = error;
			}

			this->do_complete_locked(result, prefer_apartment, true, lock2, false);
		};

		if (could_run_locally) {
			lock.unlock();
			pending_schedule_fn(); //超高优先级、且spec_partment为nullptr，则立即执行，省掉schedule过程
		}
		else {
			m_extra_intermediate_data_ptr->m_pending_schedule_id = (m_mode == ks_raw_future_mode::TASK)
				? prefer_apartment->schedule(std::move(pending_schedule_fn), priority)
				: prefer_apartment->schedule_delayed(std::move(pending_schedule_fn), priority, m_delay);
			if (m_extra_intermediate_data_ptr->m_pending_schedule_id == 0) {
				//schedule失败，则立即将this标记为错误即可
				this->do_complete_locked(ks_error::terminated_error(), nullptr, false, lock, false);
				return;
			}
		}
	}

protected:
	virtual void on_feeded_by_prev(const ks_raw_result& prev_result, ks_raw_future* prev_future, ks_apartment* prev_advice_apartment) override {
		//ks_raw_promise_future的此方法不应被调用，而是应直接do_complete
		ASSERT(false);
	}

	virtual void do_reset_extra_data_locked(std::unique_lock<ks_mutex>& lock) override {
		if (m_extra_intermediate_data_ptr != nullptr) {
			m_extra_intermediate_data_ptr->m_task_fn = {};
			m_extra_intermediate_data_ptr->m_pending_schedule_id = 0;

			m_extra_intermediate_data_ptr.reset();
		}
	}

	virtual void do_try_cancel(const ks_error& error, bool backtrack) override {
		ASSERT(error.get_code() != 0);

		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return;

		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		if (m_cancelable) {
			m_intermediate_data_ptr->m_cancel_error = error;

			//若为延时任务，则执行unschedule，task-future结果状态将成为rejected
			const bool is_delaying_schedule_task = (m_mode == ks_raw_future_mode::TASK_DELAYED && m_delay >= 0);
			if (is_delaying_schedule_task) {
				if (m_extra_intermediate_data_ptr->m_pending_schedule_id != 0) {
					m_spec_apartment->try_unschedule(m_extra_intermediate_data_ptr->m_pending_schedule_id);
					m_extra_intermediate_data_ptr->m_pending_schedule_id = 0;
				}

				this->do_complete_locked(error, nullptr, false, lock, false);
			}
		}
	}

	virtual bool is_with_upstream_future() override {
		return false;
	}

private:
	const int64_t m_delay;  //const-like

	struct __EXTRA_INTERMEDIATE_DATA {
		std::function<ks_raw_result()> m_task_fn; //在complete后被自动清除
		uint64_t m_pending_schedule_id = 0;
	};

	std::shared_ptr<__EXTRA_INTERMEDIATE_DATA> m_extra_intermediate_data_ptr;
};


class ks_raw_pipe_future final : public ks_raw_future_baseimp {
public:
	explicit ks_raw_pipe_future(ks_raw_future_mode mode, ks_apartment* spec_apartment, std::function<ks_raw_result(const ks_raw_result&)>&& fn_ex, const ks_async_context& living_context, bool cancelable)
		: ks_raw_future_baseimp(mode, cancelable, spec_apartment, living_context) {
		m_extra_intermediate_data_ptr = std::make_shared<__EXTRA_INTERMEDIATE_DATA>();
		m_extra_intermediate_data_ptr->m_fn_ex = std::move(fn_ex);
	}

	_DISABLE_COPY_CONSTRUCTOR(ks_raw_pipe_future);

	void connect(const ks_raw_future_ptr& prev_future) {
		std::unique_lock<ks_mutex> lock(m_mutex);
		ASSERT(!m_completed_result.is_completed());
		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		if (m_spec_apartment == nullptr) 
			m_spec_apartment = prev_future->get_spec_apartment();

		m_extra_intermediate_data_ptr->m_prev_future_weak = prev_future;

		lock.unlock();
		prev_future->do_add_next(this->shared_from_this());
	}

protected:
	virtual void on_feeded_by_prev(const ks_raw_result& prev_result, ks_raw_future* prev_future, ks_apartment* prev_advice_apartment) override {
		ASSERT(prev_result.is_completed());

		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return; 

		ASSERT(m_intermediate_data_ptr != nullptr);

		bool could_skip_run = false;
		switch (m_mode) {
		case ks_raw_future_mode::THEN:
			could_skip_run = !prev_result.is_value();
			break;
		case ks_raw_future_mode::TRAP:
			could_skip_run = !prev_result.is_error();
			break;
		case ks_raw_future_mode::TRANSFORM:
			could_skip_run = false;
			break;
		case ks_raw_future_mode::FORWARD:
			could_skip_run = true;
			break;
		default:
			ASSERT(false);
			break;
		}

		if (could_skip_run) {
			//可直接skip-run，则立即将this进行settle即可
			ks_apartment* prefer_apartment = this->do_determine_prefer_apartment(prev_advice_apartment);
			this->do_complete_locked(prev_result, prefer_apartment, true, lock, false);
			return;
		}

		if (m_cancelable && this->do_check_cancel_locking(lock)) {
			//若this已被cancel，则立即将this进行settle即可
			ks_apartment* prefer_apartment = this->do_determine_prefer_apartment(prev_advice_apartment);
			this->do_complete_locked(
				prev_result.is_error() ? prev_result.to_error() : this->do_acquire_cancel_error_locking(ks_error::cancelled_error(), lock),
				prefer_apartment, true, lock, false);
			return;
		}

		int priority = m_intermediate_data_ptr->m_living_context.__get_priority();
		ks_apartment* prefer_apartment = this->do_determine_prefer_apartment(prev_advice_apartment);
		bool could_run_locally = (priority >= 0x10000) && (m_spec_apartment == nullptr || m_spec_apartment == prefer_apartment);

		std::function<void()> run_fn = [this, this_shared = this->shared_from_this(), prev_result, prefer_apartment, context = m_intermediate_data_ptr->m_living_context]() mutable -> void {
			std::unique_lock<ks_mutex> lock2(m_mutex);
			if (m_completed_result.is_completed())
				return; //pre-check cancelled

			ASSERT(m_intermediate_data_ptr != nullptr);
			ASSERT(m_extra_intermediate_data_ptr != nullptr);

			m_intermediate_data_ptr->m_pending_flag_v = false;

			ks_raw_running_future_rtstt running_future_rtstt;
			ks_raw_living_context_rtstt living_context_rtstt;
			running_future_rtstt.apply(this, &tls_current_thread_running_future);
			living_context_rtstt.apply(context);

			ks_raw_result result;
			try {
				if (m_cancelable && this->do_check_cancel_locking(lock2))
					result = prev_result.is_error() ? prev_result.to_error() : this->do_acquire_cancel_error_locking(ks_error::cancelled_error(), lock2);
				else {
					std::function<ks_raw_result(const ks_raw_result&)> fn_ex = std::move(m_extra_intermediate_data_ptr->m_fn_ex);
					lock2.unlock();
					result = fn_ex(prev_result).require_completed_or_error();
					lock2.lock();
					fn_ex = {};
				}
			}
			catch (ks_error error) {
				result = error;
			}

			this->do_complete_locked(result, prefer_apartment, true, lock2, false);
		};

		if (could_run_locally) {
			lock.unlock();
			run_fn(); //超高优先级、且spec_partment为nullptr，则立即执行，省掉schedule过程
			return;
		}

		uint64_t act_schedule_id = prefer_apartment->schedule(std::move(run_fn), priority);
		if (act_schedule_id == 0) {
			//schedule失败，则立即将this标记为错误即可
			this->do_complete_locked(ks_error::terminated_error(), prefer_apartment, true, lock, false);
			return;
		}
	}

	virtual void do_reset_extra_data_locked(std::unique_lock<ks_mutex>& lock) override {
		if (m_extra_intermediate_data_ptr != nullptr) {
			m_extra_intermediate_data_ptr->m_fn_ex = {};
			m_extra_intermediate_data_ptr->m_prev_future_weak.reset();

			m_extra_intermediate_data_ptr.reset();
		}
	}

	virtual void do_try_cancel(const ks_error& error, bool backtrack) override {
		ASSERT(error.get_code() != 0);

		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return;

		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		if (m_cancelable) {
			m_intermediate_data_ptr->m_cancel_error = error;
			//pipe-future无需主动标记结果
		}

		if (backtrack) {
			ks_raw_future_ptr prev_future = m_extra_intermediate_data_ptr->m_prev_future_weak.lock();
			m_extra_intermediate_data_ptr->m_prev_future_weak.reset();

			lock.unlock();
			if (prev_future != nullptr) 
				prev_future->do_try_cancel(error, true);
		}
	}

	virtual bool is_with_upstream_future() override {
		return true;
	}

private:
	struct __EXTRA_INTERMEDIATE_DATA {
		std::function<ks_raw_result(const ks_raw_result&)> m_fn_ex;  //在complete后被自动清除
		std::weak_ptr<ks_raw_future> m_prev_future_weak;             //在complete后被自动清除
	};

	std::shared_ptr<__EXTRA_INTERMEDIATE_DATA> m_extra_intermediate_data_ptr;
};


class ks_raw_flatten_future final : public ks_raw_future_baseimp {
public:
	explicit ks_raw_flatten_future(ks_raw_future_mode mode, ks_apartment* spec_apartment, std::function<ks_raw_future_ptr(const ks_raw_result&)>&& afn_ex, const ks_async_context& living_context)
		: ks_raw_future_baseimp(mode, true, spec_apartment, living_context) {
		m_extra_intermediate_data_ptr = std::make_shared<__EXTRA_INTERMEDIATE_DATA>();
		m_extra_intermediate_data_ptr->m_afn_ex = std::move(afn_ex);
	}

	void connect(const ks_raw_future_ptr& prev_future) {
		std::unique_lock<ks_mutex> lock(m_mutex);
		ASSERT(!m_completed_result.is_completed());
		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		if (m_spec_apartment == nullptr)
			m_spec_apartment = prev_future->get_spec_apartment();

		m_extra_intermediate_data_ptr->m_prev_future_weak = prev_future;

		auto this_shared = this->shared_from_this();
		auto context = m_intermediate_data_ptr->m_living_context;

		lock.unlock();
		prev_future->on_completion(
			[this, this_shared, context](const ks_raw_result& prev_result) mutable -> void {
			std::unique_lock<ks_mutex> lock2(m_mutex);
			if (m_completed_result.is_completed()) 
				return;

			m_intermediate_data_ptr->m_pending_flag_v = false;

			ks_raw_running_future_rtstt running_future_rtstt;
			ks_raw_living_context_rtstt living_context_rtstt;
			running_future_rtstt.apply(this, &tls_current_thread_running_future);
			living_context_rtstt.apply(context);

			ks_apartment* prefer_apartment = this->do_determine_prefer_apartment(nullptr);

			ks_raw_future_ptr extern_future = nullptr;
			ks_error else_error = {};
			try {
				if (this->do_check_cancel_locking(lock2))
					else_error = prev_result.is_error() ? prev_result.to_error() : this->do_acquire_cancel_error_locking(ks_error::cancelled_error(), lock2);
				else {
					std::function<ks_raw_future_ptr(const ks_raw_result&)> afn_ex = std::move(m_extra_intermediate_data_ptr->m_afn_ex);
					lock2.unlock();
					extern_future = afn_ex(prev_result);
					if (extern_future == nullptr)
						else_error = ks_error::unexpected_error();
					lock2.lock();
					afn_ex = {};
				}
			}
			catch (ks_error error) {
				else_error = error;
			}

			if (extern_future != nullptr) {
				m_extra_intermediate_data_ptr->m_extern_future = extern_future;

				lock2.unlock();
				extern_future->on_completion([this, this_shared, prefer_apartment](const ks_raw_result& extern_result) {
					this->do_complete(extern_result, prefer_apartment, false);
				}, make_async_context().set_priority(0x10000), prefer_apartment);
			}
			else {
				this->do_complete_locked(else_error, prefer_apartment, false, lock2, false);
			}
		}, context, m_spec_apartment);
	}

protected:
	virtual void on_feeded_by_prev(const ks_raw_result& prev_result, ks_raw_future* prev_future, ks_apartment* prev_advice_apartment) override {
		//ks_raw_promise_future的此方法不应被调用，而是应直接do_complete
		ASSERT(false);
	}

	virtual void do_reset_extra_data_locked(std::unique_lock<ks_mutex>& lock) override {
		if (m_extra_intermediate_data_ptr != nullptr) {
			m_extra_intermediate_data_ptr->m_afn_ex = {};
			m_extra_intermediate_data_ptr->m_prev_future_weak.reset();
			m_extra_intermediate_data_ptr->m_extern_future.reset();

			m_extra_intermediate_data_ptr.reset();
		}
	}

	virtual void do_try_cancel(const ks_error& error, bool backtrack) override {
		ASSERT(error.get_code() != 0);

		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return;

		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		if (m_cancelable) {
			m_intermediate_data_ptr->m_cancel_error = error;
		}

		if (backtrack) {
			ks_raw_future_ptr prev_future = m_extra_intermediate_data_ptr->m_prev_future_weak.lock();
			ks_raw_future_ptr extern_future = m_extra_intermediate_data_ptr->m_extern_future;
			m_extra_intermediate_data_ptr->m_prev_future_weak.reset();
			m_extra_intermediate_data_ptr->m_extern_future.reset();

			lock.unlock();
			if (extern_future != nullptr)
				extern_future->try_cancel(backtrack);
			if (prev_future != nullptr)
				prev_future->do_try_cancel(error, backtrack);
		}
	}

	virtual bool is_with_upstream_future() override {
		return true;
	}

private:
	struct __EXTRA_INTERMEDIATE_DATA {
		std::function<ks_raw_future_ptr(const ks_raw_result&)> m_afn_ex;
		std::weak_ptr<ks_raw_future> m_prev_future_weak;
		std::shared_ptr<ks_raw_future> m_extern_future;
	};

	std::shared_ptr<__EXTRA_INTERMEDIATE_DATA> m_extra_intermediate_data_ptr;
};


class ks_raw_aggr_future final : public ks_raw_future_baseimp {
public:
	explicit ks_raw_aggr_future(ks_raw_future_mode mode, ks_apartment* spec_apartment) 
		: ks_raw_future_baseimp(mode, true, spec_apartment, ks_async_context::__empty_inst()) {
		m_extra_intermediate_data_ptr = std::make_shared<__EXTRA_INTERMEDIATE_DATA>();
	}

	_DISABLE_COPY_CONSTRUCTOR(ks_raw_aggr_future);

	void connect(const std::vector<ks_raw_future_ptr>& prev_futures) {
		std::unique_lock<ks_mutex> lock(m_mutex);
		ASSERT(!m_completed_result.is_completed());
		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		m_extra_intermediate_data_ptr->m_prev_future_weak_seq.reserve(prev_futures.size());
		m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq.reserve(prev_futures.size());
		for (auto& prev_future : prev_futures) {
			m_extra_intermediate_data_ptr->m_prev_future_weak_seq.push_back(prev_future);
			m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq.push_back(prev_future.get());
		}
		m_extra_intermediate_data_ptr->m_prev_total_count = prev_futures.size();
		m_extra_intermediate_data_ptr->m_prev_completed_count = 0;

		m_extra_intermediate_data_ptr->m_prev_result_seq_cache.resize(prev_futures.size(), ks_raw_result());
		m_extra_intermediate_data_ptr->m_prev_prefer_apartment_seq_cache.resize(prev_futures.size(), nullptr);
		m_extra_intermediate_data_ptr->m_prev_first_resolved_index = -1;
		m_extra_intermediate_data_ptr->m_prev_first_rejected_index = -1;

		ks_raw_future_ptr this_shared = this->shared_from_this();

		lock.unlock();
		for (auto& prev_future : prev_futures) 
			prev_future->do_add_next(this_shared);
	}

protected:
	virtual void on_feeded_by_prev(const ks_raw_result& prev_result, ks_raw_future* prev_future, ks_apartment* prev_advice_apartment) override {
		ASSERT(prev_result.is_completed());

		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return;

		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		size_t prev_index_just = -1;
		ASSERT(m_extra_intermediate_data_ptr->m_prev_completed_count < m_extra_intermediate_data_ptr->m_prev_total_count);
		while (true) { //此处while为了支持future重复出现
			auto prev_future_iter = std::find(m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq.cbegin(), m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq.cend(), prev_future);
			if (prev_future_iter == m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq.cend())
				break; //miss prev_future (unexpected)
			size_t prev_index = prev_future_iter - m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq.cbegin();
			if (m_extra_intermediate_data_ptr->m_prev_result_seq_cache[prev_index].is_completed())
				break; //the prev_future has been completed (unexpected)
			if (prev_index_just == -1)
				prev_index_just = prev_index;

			m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq[prev_index] = nullptr;
			m_extra_intermediate_data_ptr->m_prev_result_seq_cache[prev_index] = prev_result.require_completed_or_error();
			m_extra_intermediate_data_ptr->m_prev_prefer_apartment_seq_cache[prev_index] = prev_advice_apartment;
			m_extra_intermediate_data_ptr->m_prev_completed_count++;
			if (m_extra_intermediate_data_ptr->m_prev_first_resolved_index == -1 && prev_result.is_value())
				m_extra_intermediate_data_ptr->m_prev_first_resolved_index = prev_index;
			if (m_extra_intermediate_data_ptr->m_prev_first_rejected_index == -1 && !prev_result.is_value())
				m_extra_intermediate_data_ptr->m_prev_first_rejected_index = prev_index;
		}

		if (prev_index_just == -1) {
			ASSERT(false);
			return;
		}

		do_check_and_try_settle_me_locked(prev_result, prev_advice_apartment, lock, false);
	}

	virtual void do_reset_extra_data_locked(std::unique_lock<ks_mutex>& lock) override {
		if (m_extra_intermediate_data_ptr != nullptr) {
			m_extra_intermediate_data_ptr->m_prev_future_weak_seq.clear();
			m_extra_intermediate_data_ptr->m_prev_future_raw_pointer_seq.clear();
			m_extra_intermediate_data_ptr->m_prev_result_seq_cache.clear();
			m_extra_intermediate_data_ptr->m_prev_prefer_apartment_seq_cache.clear();

			m_extra_intermediate_data_ptr.reset();
		}
	}

	virtual void do_try_cancel(const ks_error& error, bool backtrack) override {
		ASSERT(error.get_code() != 0);

		std::unique_lock<ks_mutex> lock(m_mutex);
		if (m_completed_result.is_completed())
			return;

		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		if (m_cancelable) {
			m_intermediate_data_ptr->m_cancel_error = error;
			//aggr-future无需主动标记结果
		}

		if (backtrack) {
			std::vector<ks_raw_future_ptr> prev_future_seq;
			prev_future_seq.reserve(m_extra_intermediate_data_ptr->m_prev_future_weak_seq.size());
			for (auto& prev_future_weak : m_extra_intermediate_data_ptr->m_prev_future_weak_seq) {
				ks_raw_future_ptr prev_fut = prev_future_weak.lock();
				if (prev_fut != nullptr)
					prev_future_seq.push_back(prev_fut);
			}
			m_extra_intermediate_data_ptr->m_prev_future_weak_seq.clear();

			lock.unlock();
			for (auto& prev_fut : prev_future_seq) 
				prev_fut->do_try_cancel(error, true);
		}
	}

	virtual bool is_with_upstream_future() override {
		return true;
	}

private:
	void do_check_and_try_settle_me_locked(const ks_raw_result& prev_result, ks_apartment* prev_advice_apartment, std::unique_lock<ks_mutex>& lock, bool must_keep_locked) {
		ASSERT(!must_keep_locked);

		ASSERT(!m_completed_result.is_completed());
		ASSERT(m_intermediate_data_ptr != nullptr);
		ASSERT(m_extra_intermediate_data_ptr != nullptr);

		//check and try settle me ...
		switch (m_mode) {
		case ks_raw_future_mode::ALL:
			if (m_extra_intermediate_data_ptr->m_prev_first_rejected_index != -1) {
				//前序任务出现失败
				ks_apartment* prefer_apartment = this->do_determine_prefer_apartment_from_prev_seq_locked(prev_advice_apartment, lock);
				ks_error prev_error_first = m_extra_intermediate_data_ptr->m_prev_result_seq_cache[m_extra_intermediate_data_ptr->m_prev_first_rejected_index].to_error();
				this->do_complete_locked(prev_error_first, prefer_apartment, true, lock, must_keep_locked);
				return;
			}
			else if (m_extra_intermediate_data_ptr->m_prev_completed_count == m_extra_intermediate_data_ptr->m_prev_total_count) {
				//前序任务全部成功
				ks_apartment* prefer_apartment = this->do_determine_prefer_apartment_from_prev_seq_locked(prev_advice_apartment, lock);
				std::vector<ks_raw_value> prev_value_seq;
				prev_value_seq.reserve(m_extra_intermediate_data_ptr->m_prev_result_seq_cache.size());
				for (auto& prev_result : m_extra_intermediate_data_ptr->m_prev_result_seq_cache)
					prev_value_seq.push_back(prev_result.to_value());
				this->do_complete_locked(ks_raw_value::of<std::vector<ks_raw_value>>(std::move(prev_value_seq)), prefer_apartment, true, lock, must_keep_locked);
				return;
			}
			else {
				break;
			}

		case ks_raw_future_mode::ALL_COMPLETED:
			if (m_extra_intermediate_data_ptr->m_prev_completed_count == m_extra_intermediate_data_ptr->m_prev_total_count) {
				//前序任务全部完成（无论成功/失败）
				ks_apartment* prefer_apartment = this->do_determine_prefer_apartment_from_prev_seq_locked(prev_advice_apartment, lock);
				std::vector<ks_raw_result> prev_result_seq = m_extra_intermediate_data_ptr->m_prev_result_seq_cache;
				this->do_complete_locked(ks_raw_value::of<std::vector<ks_raw_result>>(std::move(prev_result_seq)), prefer_apartment, true, lock, must_keep_locked);
				return;
			}
			else {
				break;
			}

		case ks_raw_future_mode::ANY:
			if (m_extra_intermediate_data_ptr->m_prev_first_resolved_index != -1) {
				//前序任务出现成功
				ks_apartment* prefer_apartment = this->do_determine_prefer_apartment_from_prev_seq_locked(prev_advice_apartment, lock);
				ks_raw_value prev_value_first = m_extra_intermediate_data_ptr->m_prev_result_seq_cache[m_extra_intermediate_data_ptr->m_prev_first_resolved_index].to_value();
				this->do_complete_locked(prev_value_first, prefer_apartment, true, lock, must_keep_locked);
				return;
			}
			else if (m_extra_intermediate_data_ptr->m_prev_completed_count == m_extra_intermediate_data_ptr->m_prev_total_count) {
				//前序任务全部失败
				ASSERT(m_extra_intermediate_data_ptr->m_prev_first_rejected_index != -1);
				ks_apartment* prefer_apartment = this->do_determine_prefer_apartment_from_prev_seq_locked(prev_advice_apartment, lock);
				ks_error prev_error_first = m_extra_intermediate_data_ptr->m_prev_result_seq_cache[m_extra_intermediate_data_ptr->m_prev_first_rejected_index].to_error();
				this->do_complete_locked(prev_error_first, prefer_apartment, true, lock, must_keep_locked);
				return;
			}
			else {
				break;
			}

		default:
			ASSERT(false);
			break;
		}

		if (this->do_check_cancel_locking(lock)) {
			//还有前序future仍未完成，但若this已被cancel，则立即将this进行settle即可，不再继续等待
			ks_apartment* prefer_apartment = this->do_determine_prefer_apartment_from_prev_seq_locked(prev_advice_apartment, lock);
			this->do_complete_locked(
				prev_result.is_error() ? prev_result.to_error() : this->do_acquire_cancel_error_locking(ks_error::cancelled_error(), lock),
				prefer_apartment, true, lock, must_keep_locked);
			return;
		}
	}

	ks_apartment* do_determine_prefer_apartment_from_prev_seq_locked(ks_apartment* prev_advice_apartment, std::unique_lock<ks_mutex>& lock) const {
		if (m_spec_apartment != nullptr)
			return m_spec_apartment;

		if (prev_advice_apartment != nullptr)
			return prev_advice_apartment;

		for (auto* prev_prefer_apartment : m_extra_intermediate_data_ptr->m_prev_prefer_apartment_seq_cache) {
			if (prev_prefer_apartment != nullptr)
				return prev_prefer_apartment;
		}

		return ks_apartment::current_thread_apartment_or_default_mta();
	}

private:
	struct __EXTRA_INTERMEDIATE_DATA {
		std::vector<std::weak_ptr<ks_raw_future>> m_prev_future_weak_seq;       //在触发后被自动清除
		std::vector<ks_raw_future*> m_prev_future_raw_pointer_seq;              //在触发后被自动清除
		size_t m_prev_total_count = 0;
		size_t m_prev_completed_count = 0;

		std::vector<ks_raw_result> m_prev_result_seq_cache;           //在触发后被自动清除
		std::vector<ks_apartment*> m_prev_prefer_apartment_seq_cache; //在触发后被自动清除
		size_t m_prev_first_resolved_index = -1; //在触发后被自动清除
		size_t m_prev_first_rejected_index = -1; //在触发后被自动清除
	};

	std::shared_ptr<__EXTRA_INTERMEDIATE_DATA> m_extra_intermediate_data_ptr;
};


//ks_raw_future静态方法实现
ks_raw_future_ptr ks_raw_future::resolved(const ks_raw_value& value, ks_apartment* apartment) {
	return std::make_shared<ks_raw_promise_future>(ks_raw_future_mode::DX, apartment, ks_raw_result(value));
}

ks_raw_future_ptr ks_raw_future::rejected(const ks_error& error, ks_apartment* apartment) {
	return std::make_shared<ks_raw_promise_future>(ks_raw_future_mode::DX, apartment, ks_raw_result(error));
}

ks_raw_future_ptr ks_raw_future::__from_result(const ks_raw_result& result, ks_apartment* apartment) {
	ASSERT(result.is_completed());
	return std::make_shared<ks_raw_promise_future>(ks_raw_future_mode::DX, apartment, result.is_completed() ? result : ks_raw_result(ks_error::unexpected_error()));
}


ks_raw_future_ptr ks_raw_future::post(std::function<ks_raw_result()>&& task_fn, const ks_async_context& context, ks_apartment* apartment) {
	auto task_future = std::make_shared<ks_raw_task_future>(ks_raw_future_mode::TASK, apartment, std::move(task_fn), context, 0);
	task_future->submit();
	return task_future;
}

ks_raw_future_ptr ks_raw_future::post_delayed(std::function<ks_raw_result()>&& task_fn, const ks_async_context& context, ks_apartment* apartment, int64_t delay) {
	auto task_future = std::make_shared<ks_raw_task_future>(ks_raw_future_mode::TASK_DELAYED, apartment, std::move(task_fn), context, delay);
	task_future->submit();
	return task_future;
}


ks_raw_future_ptr ks_raw_future::all(const std::vector<ks_raw_future_ptr>& futures, ks_apartment* apartment) {
	if (futures.empty())
		return ks_raw_future::resolved(ks_raw_value::of<std::vector<ks_raw_value>>(std::vector<ks_raw_value>()), apartment);

	auto aggr_future = std::make_shared<ks_raw_aggr_future>(ks_raw_future_mode::ALL, apartment);
	aggr_future->connect(futures);
	return aggr_future;
}

ks_raw_future_ptr ks_raw_future::all_completed(const std::vector<ks_raw_future_ptr>& futures, ks_apartment* apartment) {
	if (futures.empty())
		return ks_raw_future::resolved(ks_raw_value::of<std::vector<ks_raw_result>>(std::vector<ks_raw_result>()), apartment);

	auto aggr_future = std::make_shared<ks_raw_aggr_future>(ks_raw_future_mode::ALL_COMPLETED, apartment);
	aggr_future->connect(futures);
	return aggr_future;
}

ks_raw_future_ptr ks_raw_future::any(const std::vector<ks_raw_future_ptr>& futures, ks_apartment* apartment) {
	if (futures.empty())
		return ks_raw_future::rejected(ks_error::unexpected_error(), apartment);
	if (futures.size() == 1)
		return futures.at(0);

	auto aggr_future = std::make_shared<ks_raw_aggr_future>(ks_raw_future_mode::ANY, apartment);
	aggr_future->connect(futures);
	return aggr_future;
}

void ks_raw_future::try_cancel(bool backtrack) {
	this->do_try_cancel(ks_error::cancelled_error(), backtrack);
}

bool ks_raw_future::__check_current_future_cancel(bool with_extra) {
	ks_raw_future* cur_future = tls_current_thread_running_future;
	if (cur_future != nullptr) {
		ASSERT(!cur_future->is_completed());
		if (cur_future->do_check_cancel())
			return true;
		if (with_extra) {
			ks_apartment* cur_apartment = ks_apartment::current_thread_apartment();
			if (cur_apartment != nullptr) {
				if (cur_apartment->is_stopping_or_stopped())
					return true;
			}
		}
	}
	return false;
}

ks_error ks_raw_future::__acquire_current_future_cancel_error(const ks_error& def_error, bool with_extra) {
	ks_raw_future* cur_future = tls_current_thread_running_future;
	if (cur_future != nullptr) {
		ASSERT(!cur_future->is_completed());
		ks_error error = cur_future->do_acquire_cancel_error(ks_error());
		if (error.get_code() != 0)
			return error;
		if (with_extra) {
			ks_apartment* cur_apartment = ks_apartment::current_thread_apartment();
			if (cur_apartment != nullptr) {
				if (cur_apartment->is_stopping_or_stopped())
					return ks_error::terminated_error();
			}
		}
	}
	return def_error;
}

void ks_raw_future::set_timeout(int64_t timeout, bool backtrack) {
	return this->do_set_timeout(timeout, ks_error::timeout_error(), backtrack); 
}

void ks_raw_future::__wait() {
	return (void)this->do_wait();
}

ks_raw_promise_ptr ks_raw_promise::create(ks_apartment* apartment) {
	return std::make_shared<ks_raw_promise_future>(ks_raw_future_mode::PROMISE, apartment, nullptr);
}



//ks_raw_future基础pipe方法实现
ks_raw_future_ptr ks_raw_future_baseimp::then(std::function<ks_raw_result(const ks_raw_value &)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	std::function<ks_raw_result(const ks_raw_result&)> fn_ex = [fn = std::move(fn)](const ks_raw_result& input)->ks_raw_result {
		if (input.is_value())
			return fn(input.to_value());
		else
			return input;
	};

	auto pipe_future = std::make_shared<ks_raw_pipe_future>(ks_raw_future_mode::THEN, apartment, std::move(fn_ex), context, true);
	pipe_future->connect(this->shared_from_this());
	return pipe_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::trap(std::function<ks_raw_result(const ks_error &)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	std::function<ks_raw_result(const ks_raw_result&)> fn_ex = [fn = std::move(fn)](const ks_raw_result& input)->ks_raw_result {
		if (input.is_error())
			return fn(input.to_error());
		else
			return input;
	};

	auto pipe_future = std::make_shared<ks_raw_pipe_future>(ks_raw_future_mode::TRAP, apartment, std::move(fn_ex), context, true);
	pipe_future->connect(this->shared_from_this());
	return pipe_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::transform(std::function<ks_raw_result(const ks_raw_result &)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	auto pipe_future = std::make_shared<ks_raw_pipe_future>(ks_raw_future_mode::TRANSFORM, apartment, std::move(fn), context, true);
	pipe_future->connect(this->shared_from_this());
	return pipe_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::flat_then(std::function<ks_raw_future_ptr(const ks_raw_value&)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	std::function<ks_raw_future_ptr(const ks_raw_result&)> afn_ex = [fn = std::move(fn), apartment](const ks_raw_result& input)->ks_raw_future_ptr {
		if (!input.is_value())
			return ks_raw_future::rejected(input.to_error(), apartment);

		ks_raw_future_ptr extern_future = fn(input.to_value());
		ASSERT(extern_future != nullptr);
		return extern_future;
	};

	auto flatten_future = std::make_shared<ks_raw_flatten_future>(ks_raw_future_mode::FLATTEN_THEN, apartment, std::move(afn_ex), context);
	flatten_future->connect(this->shared_from_this());
	return flatten_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::flat_trap(std::function<ks_raw_future_ptr(const ks_error&)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	std::function<ks_raw_future_ptr(const ks_raw_result&)> afn_ex = [fn = std::move(fn), apartment](const ks_raw_result& input)->ks_raw_future_ptr {
		if (!input.is_error())
			return ks_raw_future::resolved(input.to_value(), apartment);

		ks_raw_future_ptr extern_future = fn(input.to_error());
		ASSERT(extern_future != nullptr);
		return extern_future;
	};

	auto flatten_future = std::make_shared<ks_raw_flatten_future>(ks_raw_future_mode::FLATTEN_TRAP, apartment, std::move(afn_ex), context);
	flatten_future->connect(this->shared_from_this());
	return flatten_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::flat_transform(std::function<ks_raw_future_ptr(const ks_raw_result&)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	auto flatten_future = std::make_shared<ks_raw_flatten_future>(ks_raw_future_mode::FLATTEN_TRANSFORM, apartment, std::move(fn), context);
	flatten_future->connect(this->shared_from_this());
	return flatten_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::on_success(std::function<void(const ks_raw_value&)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	auto fn_ex = [fn = std::move(fn)](const ks_raw_result& input)->ks_raw_result {
		if (input.is_value())
			fn(input.to_value());
		return input;
	};

	auto pipe_future = std::make_shared<ks_raw_pipe_future>(ks_raw_future_mode::THEN, apartment, std::move(fn_ex), context, false);
	pipe_future->connect(this->shared_from_this());
	return pipe_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::on_failure(std::function<void(const ks_error&)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	auto fn_ex = [fn = std::move(fn)](const ks_raw_result& input)->ks_raw_result {
		if (input.is_error())
			fn(input.to_error());
		return input;
	};

	auto pipe_future = std::make_shared<ks_raw_pipe_future>(ks_raw_future_mode::TRAP, apartment, std::move(fn_ex), context, false);
	pipe_future->connect(this->shared_from_this());
	return pipe_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::on_completion(std::function<void(const ks_raw_result&)>&& fn, const ks_async_context& context, ks_apartment* apartment) {
	auto fn_ex = [fn = std::move(fn)](const ks_raw_result& input)->ks_raw_result {
		fn(input);
		return input;
	};

	auto pipe_future = std::make_shared<ks_raw_pipe_future>(ks_raw_future_mode::TRANSFORM, apartment, std::move(fn_ex), context, false);
	pipe_future->connect(this->shared_from_this());
	return pipe_future;
}

ks_raw_future_ptr ks_raw_future_baseimp::noop(ks_apartment* apartment) {
	auto pipe_future = std::make_shared<
		ks_raw_pipe_future>(
			ks_raw_future_mode::FORWARD,
			apartment,
			[](const auto& input) { return input; },
			make_async_context().set_priority(0x10000),
			false);
	pipe_future->connect(this->shared_from_this());
	return pipe_future;
}


__KS_ASYNC_RAW_END
