﻿/* Copyright 2025 The Kingsoft's ks-async/ktl Authors. All Rights Reserved.

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

#ifndef __KS_LATCH_STD_DEF
#define __KS_LATCH_STD_DEF

#include "ks_concurrency_helper.h"
#include <mutex>
#include <condition_variable>


namespace _KSConcurrencyImpl {

class ks_latch_std {
public:
    explicit ks_latch_std(const ptrdiff_t expected)
        : m_counter(expected) {
        ASSERT(expected >= 0);
    }

    _DISABLE_COPY_CONSTRUCTOR(ks_latch_std);

    void add(const ptrdiff_t update = 1) {
        std::unique_lock<std::mutex> lock(m_mutex);
        ASSERT(update >= 0);
        m_counter += update;
    }

    void count_down(const ptrdiff_t update = 1) {
        std::unique_lock<std::mutex> lock(m_mutex);
		ASSERT(update >= 0 && update <= m_counter);
        m_counter -= update;
        if (m_counter == 0) {
            m_cv.notify_all();
        }
    }

    void wait() {
        std::unique_lock<std::mutex> lock(m_mutex);
        while (m_counter != 0) {
            m_cv.wait(lock);
        }
    }

    _NODISCARD bool try_wait() {
		std::unique_lock<std::mutex> lock(m_mutex);
		return (m_counter == 0);
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cv;
    ptrdiff_t m_counter;
};

} // namespace _KSConcurrencyImpl

#endif // __KS_LATCH_STD_DEF
