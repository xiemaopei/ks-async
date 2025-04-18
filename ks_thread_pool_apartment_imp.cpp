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

#include "ks_thread_pool_apartment_imp.h"
#include "ktl/ks_defer.h"
#include <thread>
#include <algorithm>
#include <sstream>

void __forcelink_to_ks_thread_pool_apartment_imp_cpp() {}

static thread_local uint64_t tls_current_now_thread_sn = 0;
static bool tls_current_now_thread_busy_for_idle_flag = false;


ks_thread_pool_apartment_imp::ks_thread_pool_apartment_imp(const char* name, size_t max_thread_count, uint flags) : m_d(std::make_shared<_THREAD_POOL_APARTMENT_DATA>()) {
	ASSERT(name != nullptr);
	ASSERT(max_thread_count >= 1);

	m_d->name = name;
	m_d->max_thread_count = max_thread_count;
	m_d->flags = flags;

	if ((m_d->flags & auto_register_flag) && !m_d->name.empty()) {
		ks_apartment::__register_public_apartment(m_d->name.c_str(), this);
	}
}

ks_thread_pool_apartment_imp::~ks_thread_pool_apartment_imp() {
	ASSERT(m_d->state_v == _STATE::NOT_START || m_d->state_v == _STATE::STOPPED);
	if (m_d->state_v != _STATE::STOPPED) {
		this->async_stop();
		//this->wait();  //这里不等了，因为在进程退出时导致自动析构的话，可能时机就太晚，work线程已经被杀了
	}

	if ((m_d->flags & endless_instance_flag) == 0) {
		if ((m_d->flags & auto_register_flag) && !m_d->name.empty()) {
			ks_apartment::__unregister_public_apartment(m_d->name.c_str(), this);
		}
	}
}


const char* ks_thread_pool_apartment_imp::name() {
	return m_d->name.c_str();
}

uint ks_thread_pool_apartment_imp::features() {
	uint my_features = atfork_enabled_future | nested_pump_enabled_future;
	if (m_d->max_thread_count == 1)
		my_features |= sequential_feature;
	return my_features;
}


bool ks_thread_pool_apartment_imp::start() {
	std::unique_lock<ks_mutex> lock(m_d->mutex);
	if (m_d->state_v != _STATE::NOT_START && m_d->state_v != _STATE::RUNNING)
		return false;

	_try_start_locked(lock);

	return true;
}

void ks_thread_pool_apartment_imp::async_stop() {
	std::unique_lock<ks_mutex> lock(m_d->mutex);
	return _try_stop_locked(lock);
}

//注：目前的wait实现暂不支持并发重入
void ks_thread_pool_apartment_imp::wait() {
	ASSERT(this != ks_apartment::current_thread_apartment());

	std::unique_lock<ks_mutex> lock(m_d->mutex);
	this->_try_stop_locked(lock); //ensure stop

	ASSERT(m_d->state_v == _STATE::STOPPING || m_d->state_v == _STATE::STOPPED);
	while (m_d->state_v == _STATE::STOPPING) {
		m_d->stopped_state_cv.wait(lock);
	}

	ASSERT(m_d->state_v == _STATE::STOPPED);
	ASSERT(m_d->now_fn_queue_prior.empty() && m_d->now_fn_queue_normal.empty());
}

bool ks_thread_pool_apartment_imp::is_stopped() {
	_STATE state = m_d->state_v;
	return state == _STATE::STOPPED;
}

bool ks_thread_pool_apartment_imp::is_stopping_or_stopped() {
	_STATE state = m_d->state_v;
	return state == _STATE::STOPPED || state == _STATE::STOPPING;
}


uint64_t ks_thread_pool_apartment_imp::schedule(std::function<void()>&& fn, int priority) {
	std::unique_lock<ks_mutex> lock(m_d->mutex);

	_try_start_locked(lock);
	if (m_d->state_v != _STATE::RUNNING && m_d->state_v != _STATE::STOPPING) {
		ASSERT(false);
		return 0;
	}

	uint64_t id = ++m_d->atomic_last_fn_id;
	ASSERT(id != 0);
	ASSERT(!_debug_check_fn_id_exists_locked(m_d, id, lock));

	_FN_ITEM fn_item;
	fn_item.fn = std::move(fn);
	fn_item.priority = priority;
	fn_item.is_delaying_fn = false;
	fn_item.delay = 0;
	fn_item.until_time = std::chrono::steady_clock::time_point{};
	fn_item.fn_id = id;

	_do_put_fn_item_into_now_list_locked(m_d, std::move(fn_item), lock);
	_prepare_now_thread_pool_locked(this, m_d, lock);

	return id;
}

uint64_t ks_thread_pool_apartment_imp::schedule_delayed(std::function<void()>&& fn, int priority, int64_t delay) {
	std::unique_lock<ks_mutex> lock(m_d->mutex);

	_try_start_locked(lock);
	if (m_d->state_v != _STATE::RUNNING) {
		ASSERT(false);
		return 0;
	}

	uint64_t id = ++m_d->atomic_last_fn_id;
	ASSERT(id != 0);
	ASSERT(!_debug_check_fn_id_exists_locked(m_d, id, lock));

	_FN_ITEM fn_item;
	fn_item.fn = std::move(fn);
	fn_item.priority = priority;
	fn_item.is_delaying_fn = true;
	fn_item.delay = delay;
	fn_item.until_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(delay);
	fn_item.fn_id = id;

	if (delay <= 0) {
		_do_put_fn_item_into_now_list_locked(m_d, std::move(fn_item), lock);
		_prepare_now_thread_pool_locked(this, m_d, lock);
	}
	else {
		_do_put_fn_item_into_delaying_list_locked(m_d, std::move(fn_item), lock);
		_prepare_delaying_trigger_thread_locked(this, m_d, lock);
	}

	return id;
}

void ks_thread_pool_apartment_imp::try_unschedule(uint64_t id) {
	if (id == 0)
		return;

	std::unique_lock<ks_mutex> lock(m_d->mutex);

	auto do_erase_fn_from = [](std::deque<_FN_ITEM>*fn_queue, uint64_t fn_id) -> bool {
		auto it = std::find_if(fn_queue->begin(), fn_queue->end(),
			[fn_id](const auto& item) {return item.fn_id == fn_id; });
		if (it != fn_queue->end()) {
			fn_queue->erase(it);
			return true;
		}
		else {
			return false;
		}
	};

	//检查延时任务队列
	if (do_erase_fn_from(&m_d->delaying_fn_queue, id))
		return;
	//检查idle任务队列
	if (do_erase_fn_from(&m_d->now_fn_queue_idle, id))
		return;

	//对于其他任务队列（normal和prior），没有检查的必要和意义
	return;
}


void ks_thread_pool_apartment_imp::_try_start_locked(std::unique_lock<ks_mutex>& lock) {
	if (m_d->state_v == _STATE::NOT_START) {
		m_d->state_v = _STATE::RUNNING;
	}
}

void ks_thread_pool_apartment_imp::_try_stop_locked(std::unique_lock<ks_mutex>& lock) {
	if (m_d->state_v == _STATE::RUNNING) {
		if (!(m_d->thread_pool.empty() && m_d->delaying_trigger_thread_opt == nullptr)) {
			m_d->state_v = _STATE::STOPPING;
			m_d->now_fn_queue_cv.notify_all(); //trigger now-threads
			m_d->delaying_fn_queue_cv.notify_all(); //trigger delaying-thread
		}
		else {
			m_d->state_v = _STATE::STOPPED;
			m_d->stopped_state_cv.notify_all();
		}
	}
	else if (m_d->state_v == _STATE::NOT_START) {
		m_d->state_v = _STATE::STOPPED;
		m_d->stopped_state_cv.notify_all();
	}
}


void ks_thread_pool_apartment_imp::_prepare_now_thread_pool_locked(ks_thread_pool_apartment_imp* self, const std::shared_ptr<_THREAD_POOL_APARTMENT_DATA>& d, std::unique_lock<ks_mutex>& lock) {
	if (d->thread_pool.size() >= d->max_thread_count)
		return;

	size_t needed_thread_count = 0;
	if (d->state_v == _STATE::RUNNING) {
		needed_thread_count = d->thread_pool.size() + d->now_fn_queue_prior.size() + d->now_fn_queue_normal.size() + d->now_fn_queue_idle.size();
		if (needed_thread_count > d->max_thread_count)
			needed_thread_count = d->max_thread_count;
		if (needed_thread_count == 0)
			needed_thread_count = 1;
	}
	else if (d->state_v == _STATE::STOPPING) {
		needed_thread_count = d->thread_pool.size() + d->now_fn_queue_prior.size() + d->now_fn_queue_normal.size();
		if (needed_thread_count > d->max_thread_count)
			needed_thread_count = d->max_thread_count;
		if (needed_thread_count == 0)
			needed_thread_count = 1;
	}

	if (d->thread_pool.size() < needed_thread_count) {
		for (size_t i = d->thread_pool.size(); i < needed_thread_count; ++i) {
			_THREAD_ITEM thread_item;
			thread_item.thread_sn = ++d->atomic_last_thread_sn;
			d->thread_pool.push_back(thread_item);
			d->living_any_thread_total++;

			std::thread([self, d, thread_sn = thread_item.thread_sn]() {
				_now_thread_proc(self, d, thread_sn); 
			}).detach();
		}
	}
}

void ks_thread_pool_apartment_imp::_now_thread_proc(ks_thread_pool_apartment_imp* self, const std::shared_ptr<_THREAD_POOL_APARTMENT_DATA>& d, uint64_t thread_sn) {
	ASSERT(ks_apartment::current_thread_apartment() == nullptr);
	ASSERT(tls_current_now_thread_sn == 0);
	ks_apartment::__set_current_thread_apartment(self);
	tls_current_now_thread_sn = thread_sn;

	if (true) {
		std::stringstream thread_name_ss;
		thread_name_ss << d->name << "'s work-thread [" << thread_sn << "/" << d->max_thread_count << "]";
		ks_apartment::__set_current_thread_name(thread_name_ss.str().c_str());
	}

	while (true) {
		std::unique_lock<ks_mutex> lock(d->mutex);

#if __KS_APARTMENT_ATFORK_ENABLED
		while (d->atforking_flag) {
			d->atforking_done_cv.wait(lock);
		}
#endif

#if __KS_APARTMENT_ATFORK_ENABLED
		++d->working_rc;

		ks_defer defer_dec_working_rc([&d, &lock]() {
			ASSERT(lock.owns_lock());
			if (--d->working_rc == 0)
				d->working_done_cv.notify_all();
		});
#endif

		//try next now_fn
		auto* now_fn_queue_sel = !d->now_fn_queue_prior.empty() ? &d->now_fn_queue_prior : &d->now_fn_queue_normal;
		if (now_fn_queue_sel->empty()) {
			if (d->state_v != _STATE::RUNNING && d->now_fn_queue_prior.empty() && d->now_fn_queue_normal.empty())
				break; //check stop, end

			//保留1个线程不去执行idle任务（除非是单线程套间）
			if (!d->now_fn_queue_idle.empty()
				&& (d->busy_thread_count_for_idle == 0 || d->busy_thread_count_for_idle + 1 < d->thread_pool.size()))
				now_fn_queue_sel = &d->now_fn_queue_idle;
		}

		if (!now_fn_queue_sel->empty()) {
			//pop and exec a fn
			_FN_ITEM now_fn_item = std::move(now_fn_queue_sel->front());
			now_fn_queue_sel->pop_front();

			const bool busy_for_idle_flag = (now_fn_queue_sel == &d->now_fn_queue_idle);
			if (busy_for_idle_flag) {
				++d->busy_thread_count_for_idle;
				tls_current_now_thread_busy_for_idle_flag = true;
			}

			ks_defer defer_dec_busy_thread_count([&d, busy_for_idle_flag, &lock]() {
				ASSERT(lock.owns_lock());
				if (busy_for_idle_flag) {
					--d->busy_thread_count_for_idle;
					tls_current_now_thread_busy_for_idle_flag = false;
				}
			});

			lock.unlock();
			now_fn_item.fn();

			lock.lock(); //for working_rc and busy_thread_count
			continue;
		}
		else {
			//wait
			d->now_fn_queue_cv.wait(lock);
			continue;
		}
	}

	if (true) {
		std::unique_lock<ks_mutex> lock(d->mutex);

		ASSERT(d->living_any_thread_total > 0);
		d->living_any_thread_total--;

		if (d->state_v == _STATE::STOPPING && d->living_any_thread_total == 0) {
			d->state_v = _STATE::STOPPED;
			d->stopped_state_cv.notify_all();
		}
	}

	ASSERT(ks_apartment::current_thread_apartment() == self);
	ASSERT(tls_current_now_thread_sn == thread_sn);
}

void ks_thread_pool_apartment_imp::_prepare_delaying_trigger_thread_locked(ks_thread_pool_apartment_imp* self, const std::shared_ptr<_THREAD_POOL_APARTMENT_DATA>& d, std::unique_lock<ks_mutex>& lock) {
	if (d->delaying_trigger_thread_opt != nullptr)
		return;

	if (d->state_v == _STATE::RUNNING) {
		d->delaying_trigger_thread_opt = std::make_shared<_DELAYING_THREAD_ITEM>();
		d->living_any_thread_total++;

		std::thread([self, d]() { 
			_delaying_trigger_thread_proc(self, d); 
		}).detach();
	}
}

void ks_thread_pool_apartment_imp::_delaying_trigger_thread_proc(ks_thread_pool_apartment_imp* self, const std::shared_ptr<_THREAD_POOL_APARTMENT_DATA>& d) {
	if (true) {
		std::stringstream thread_name_ss;
		thread_name_ss << d->name << "'s time-thread";
		ks_apartment::__set_current_thread_name(thread_name_ss.str().c_str());
	}

	while (true) {
		std::unique_lock<ks_mutex> lock(d->mutex);

#if __KS_APARTMENT_ATFORK_ENABLED
		while (d->atforking_flag)
			d->atforking_done_cv.wait(lock);
#endif

		//try next tilled delayed_fn
		if (d->state_v != _STATE::RUNNING)
			break; //check stop, end

		if (d->delaying_fn_queue.empty()) {
			//wait
			d->delaying_fn_queue_cv.wait(lock);
			continue;
		}
		else if (d->delaying_fn_queue.front().until_time > std::chrono::steady_clock::now()) {
			//wait until
			d->delaying_fn_queue_cv.wait_until(lock, d->delaying_fn_queue.front().until_time);
			continue;
		}
		else {
			//直接将到期的delaying项移入idle队列（忽略priority）
			_do_put_fn_item_into_now_list_locked(d, std::move(d->delaying_fn_queue.front()), lock);
			d->delaying_fn_queue.pop_front();
			_prepare_now_thread_pool_locked(self, d, lock);
			continue;
		}
	}

	if (true) {
		std::unique_lock<ks_mutex> lock(d->mutex);

		ASSERT(d->living_any_thread_total > 0);
		d->living_any_thread_total--;

		if (d->state_v == _STATE::STOPPING && d->living_any_thread_total == 0) {
			d->state_v = _STATE::STOPPED;
			d->stopped_state_cv.notify_all();
		}
	}
}

bool ks_thread_pool_apartment_imp::_debug_check_fn_id_exists_locked(const std::shared_ptr<_THREAD_POOL_APARTMENT_DATA>& d, uint64_t id, std::unique_lock<ks_mutex>& lock) {
	auto do_check_fn_exists = [](std::deque<_FN_ITEM>* fn_queue, uint64_t fn_id) -> bool {
		return std::find_if(fn_queue->cbegin(), fn_queue->cend(), 
			[fn_id](const auto& item) {return item.fn_id == fn_id; }) != fn_queue->cend();
	};

	return do_check_fn_exists(&d->now_fn_queue_prior, id)
		|| do_check_fn_exists(&d->now_fn_queue_normal, id)
		|| do_check_fn_exists(&d->now_fn_queue_idle, id)
		|| do_check_fn_exists(&d->delaying_fn_queue, id);
}

void ks_thread_pool_apartment_imp::_do_put_fn_item_into_now_list_locked(const std::shared_ptr<_THREAD_POOL_APARTMENT_DATA>& d, _FN_ITEM&& fn_item, std::unique_lock<ks_mutex>& lock) {
	auto* now_fn_queue_sel = 
		fn_item.is_delaying_fn ? &d->now_fn_queue_idle :
		fn_item.priority == 0 ? &d->now_fn_queue_normal :  //priority=0为普通优先级
		fn_item.priority > 0 ? &d->now_fn_queue_prior :    //priority>0为高优先级
		&d->now_fn_queue_idle;                             //priority<0为低优先级，简单地加入到idle队列
	now_fn_queue_sel->push_back(std::move(fn_item));
	d->now_fn_queue_cv.notify_one();
}

void ks_thread_pool_apartment_imp::_do_put_fn_item_into_delaying_list_locked(const std::shared_ptr<_THREAD_POOL_APARTMENT_DATA>& d, _FN_ITEM&& fn_item, std::unique_lock<ks_mutex>& lock) {
	auto where_it = d->delaying_fn_queue.end();
	if (!d->delaying_fn_queue.empty()) {
		if (fn_item.until_time >= d->delaying_fn_queue.back().until_time) {
			//最大延时项：插在队尾
			where_it = d->delaying_fn_queue.end();
		}
		else if (fn_item.until_time < d->delaying_fn_queue.front().until_time) {
			//最小延时项：插在队头
			where_it = d->delaying_fn_queue.begin();
		}
		else {
			//新项的目标时点落在当前队列的时段区间内。。。
			//忽略priority
			where_it = std::upper_bound(d->delaying_fn_queue.begin(), d->delaying_fn_queue.end(), fn_item,
				[](const auto& a, const auto& b) { return a.until_time < b.until_time; });
		}
	}

	bool should_notify = d->delaying_fn_queue.empty() || fn_item.until_time < d->delaying_fn_queue.front().until_time;
	d->delaying_fn_queue.insert(where_it, std::move(fn_item));

	if (should_notify) {
		//只需notify_one即可，即使有多项。
		//这是因为调度时到期的delayed项会被先移至now队列，即使瞬间由一个线程处理多项也没什么负担。
		//参见_thread_proc中对于delayed的调度算法。
		d->delaying_fn_queue_cv.notify_one();
	}
}


#if __KS_APARTMENT_ATFORK_ENABLED
void ks_thread_pool_apartment_imp::atfork_prepare() {
	ASSERT(m_d->state_v != _STATE::STOPPING && m_d->state_v != _STATE::STOPPED);
	if (m_d->atforking_flag)
		return; //重复prepare

	const bool atfork_calling_in_my_thread_flag = (ks_apartment::current_thread_apartment() == this);
	_UNUSED(atfork_calling_in_my_thread_flag); //即使在该mta某个线程内调用fork，也要照样lock，因为至少还有delaying线程呢

	std::unique_lock<ks_mutex> lock(m_d->mutex);
	m_d->atforking_flag = true;

	m_d->now_fn_queue_cv.notify_all();
	m_d->delaying_fn_queue_cv.notify_all();

	while (m_d->working_rc != 0)
		m_d->working_done_cv.wait(lock);

	lock.release();
}

void ks_thread_pool_apartment_imp::atfork_parent() {
	ASSERT(m_d->state_v != _STATE::STOPPING && m_d->state_v != _STATE::STOPPED);
	if (!m_d->atforking_flag)
		return;

	const bool atfork_calling_in_my_thread_flag = (ks_apartment::current_thread_apartment() == this);
	_UNUSED(atfork_calling_in_my_thread_flag); //即使在该mta某个线程内调用fork，也要照样lock，因为至少还有delaying线程呢

	m_d->atforking_flag = false;
	m_d->atforking_done_cv.notify_all();
	m_d->mutex.unlock();
}

void ks_thread_pool_apartment_imp::atfork_child() {
	ASSERT(m_d->state_v != _STATE::STOPPING && m_d->state_v != _STATE::STOPPED);
	if (!m_d->atforking_flag)
		return;

	const bool atfork_calling_in_my_thread_flag = (ks_apartment::current_thread_apartment() == this);
	_UNUSED(atfork_calling_in_my_thread_flag); //即使在该mta某个线程内调用fork，也要照样lock，因为至少还有delaying线程呢

	//重建线程
	if (!m_d->thread_pool.empty()) {
		uint64_t current_now_thread_sn = atfork_calling_in_my_thread_flag ? tls_current_now_thread_sn : 0;
		for (auto& thread_item : m_d->thread_pool) {
			if (!atfork_calling_in_my_thread_flag || thread_item.thread_sn != current_now_thread_sn) {
				std::thread([self = this, d = m_d, thread_sn = thread_item.thread_sn]() {
					_now_thread_proc(self, d, thread_sn);
				}).detach();
			}
		}
	}

	if (m_d->delaying_trigger_thread_opt != nullptr) {
		std::thread([self = this, d = m_d]() {
			_delaying_trigger_thread_proc(self, d); 
		}).detach();
	}

	m_d->atforking_flag = false;
	m_d->atforking_done_cv.notify_all();
	m_d->mutex.unlock();
}
#endif


bool ks_thread_pool_apartment_imp::__do_run_nested_pump_loop_for_extern_waiting(void* object, std::function<bool()>&& extern_pred_fn) {
	auto d = m_d;
	bool was_satisified = false;

	ASSERT(ks_apartment::current_thread_apartment() == this);

	while (true) {
		if (extern_pred_fn()) {
			was_satisified = true;
			break; //waiting was satisfied, ok
		}

		std::unique_lock<ks_mutex> lock(d->mutex);

#if __KS_APARTMENT_ATFORK_ENABLED
		if (d->atforking_flag)
			break; //atforking, broken
#endif

#if __KS_APARTMENT_ATFORK_ENABLED
		ASSERT(d->working_rc != 0);
#endif

		//try next now_fn
		auto* now_fn_queue_sel = !d->now_fn_queue_prior.empty() ? &d->now_fn_queue_prior : &d->now_fn_queue_normal;
		if (now_fn_queue_sel->empty()) {
			if (d->state_v != _STATE::RUNNING && d->now_fn_queue_prior.empty() && d->now_fn_queue_normal.empty())
				break; //check stop, end

			//保留1个线程不去执行idle任务（除非是单线程套间）
			if (!d->now_fn_queue_idle.empty()
				&& (d->busy_thread_count_for_idle == 0 || d->busy_thread_count_for_idle + 1 < d->thread_pool.size()))
				now_fn_queue_sel = &d->now_fn_queue_idle;
		}

		if (!now_fn_queue_sel->empty()) {
			//pop and exec a fn
			_FN_ITEM now_fn_item = std::move(now_fn_queue_sel->front());
			now_fn_queue_sel->pop_front();

			const bool busy_for_idle_flag_promoting = (now_fn_queue_sel == &d->now_fn_queue_idle) && !tls_current_now_thread_busy_for_idle_flag;
			if (busy_for_idle_flag_promoting) {
				++d->busy_thread_count_for_idle;
				tls_current_now_thread_busy_for_idle_flag = true;
			}

			ks_defer defer_dec_busy_thread_count([&d, busy_for_idle_flag_promoting, &lock]() {
				ASSERT(lock.owns_lock());
				if (busy_for_idle_flag_promoting) {
					--d->busy_thread_count_for_idle;
					tls_current_now_thread_busy_for_idle_flag = false;
				}
			});

			lock.unlock();
			now_fn_item.fn();

			lock.lock(); //for working_rc and busy_thread_count
			continue;
		}
		else {
			//wait
			d->now_fn_queue_cv.wait(lock);
			continue;
		}
	}

	return was_satisified;
}

void ks_thread_pool_apartment_imp::__do_notify_nested_pump_loop_for_extern_waiting(void* object) {
	m_d->now_fn_queue_cv.notify_all();
}
