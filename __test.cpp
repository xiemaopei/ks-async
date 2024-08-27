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

// ConsoleApplication3.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include "ks_future.h"
#include "ks_promise.h"
#include "ks_async_task.h"
#include "ks_notification_center.h"
#include <iostream>
#include <sstream>
#include <string>


namespace {
    template <class T>
    std::string _result_to_str(const ks_result<T>& result) {
        if (result.is_value())
            return (std::stringstream() << result.to_value()).str();
        else if (result.is_error())
            return (std::stringstream() << "(Err:" << result.to_error().get_code() << ")").str();
        else
            return "(!Fin)";
    }
    template <>
    std::string _result_to_str(const ks_result<void>& result) {
        if (result.is_value())
            return "VOID";
        else if (result.is_error())
            return (std::stringstream() << "(Err:" << result.to_error().get_code() << ")").str();
        else
            return "(!Fin)";
    }

    template <class T>
    void _output_result(const char* title, const ks_result<T>& result) {
        if (result.is_value())
            std::cout << title << _result_to_str(result) << " (succ)\n";
        else if (result.is_error())
            std::cout << title << "{ code: " << result.to_error().get_code() << " } (error)\n";
        else
            std::cout << title << "-- (not-completed)\n";
    }

} //namespace


ks_latch g_exit_latch(0);


void test_promise() {
    g_exit_latch.add(1);
    std::cout << "test promise ... ";

    auto promise = ks_promise<std::string>::create();
    promise.get_future()
        .on_completion(ks_apartment::default_mta(), {}, [](auto& result) {
            _output_result("completion: ", result);
            g_exit_latch.count_down();
        });

    std::thread([promise]() {
        promise.resolve("pass");
    }).join();

    g_exit_latch.wait();
}

void test_post() {
    g_exit_latch.add(1);
    std::cout << "test post ... ";

    auto future = ks_future<std::string>::post(ks_apartment::default_mta(), {}, []() {
        return std::string("pass");
    });

    future.on_completion(ks_apartment::default_mta(), {}, [](auto& result) {
        _output_result("completion: ", result);
        g_exit_latch.count_down();
    });

    g_exit_latch.wait();
}

void test_post_pending() {
    g_exit_latch.add(1);
    std::cout << "test post_pending ... ";

    ks_pending_trigger trigger;
    auto future = ks_future<std::string>::post_pending(ks_apartment::default_mta(), {}, []() {
        return std::string("pass");
        }, & trigger);

    future.on_completion(ks_apartment::default_mta(), {}, [](auto& result) {
        _output_result("completion: ", result);
        g_exit_latch.count_down();
        });

    trigger.start();
    g_exit_latch.wait();
}

void test_post_delayed() {
    g_exit_latch.add(1);
    std::cout << "test post_delayed (400ms) ... ";

    const auto post_time = std::chrono::steady_clock::now();
    auto future = ks_future<std::string>::post_delayed(ks_apartment::default_mta(), {}, [post_time]() {
        auto duration = std::chrono::steady_clock::now() - post_time;
        int64_t real_delay = (int64_t)std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        std::stringstream ss;
        ss << "pass (" << real_delay << "ms)";
        return ss.str();
    }, 400);

    future.on_completion(ks_apartment::default_mta(), {}, [](auto& result) {
        _output_result("completion: ", result);
        g_exit_latch.count_down();
    });

    g_exit_latch.wait();
}

void test_all() {
    g_exit_latch.add(1);
    std::cout << "test all ... ";

    auto f1 = ks_future<std::string>::post_delayed(ks_apartment::default_mta(), {}, []() -> std::string {
        return "a";
    }, 100);
    auto f2 = ks_future<std::string>::post_delayed(ks_apartment::default_mta(), {}, []() -> std::string {
        return "b";
    }, 80);
    auto f3 = ks_future<int>::post_delayed(ks_apartment::default_mta(), {}, []() -> int {
        return 3;
    }, 120);

    ks_future_util::all(f1, f2, f3)
        .then<std::string>(ks_apartment::default_mta(), {}, [](const std::tuple<std::string, std::string, int>& valueTuple) -> std::string {
            std::stringstream ss;
            ss << std::get<0>(valueTuple) << std::get<1>(valueTuple) << std::get<2>(valueTuple);
            return ss.str();
        })
        .on_completion(ks_apartment::default_mta(), {}, [](auto& result) {
            _output_result("completion: ", result);
            g_exit_latch.count_down();
        });

    g_exit_latch.wait();
}

void test_any() {
    g_exit_latch.add(1);
    std::cout << "test any ... ";

    auto f1 = ks_future<std::string>::post_delayed(ks_apartment::default_mta(), {}, []() -> std::string {
        return "a";
    }, 100);
    auto f2 = ks_future<std::string>::post_delayed(ks_apartment::default_mta(), {}, []() -> std::string {
        return "b";
    }, 80);
    auto f3 = ks_future<std::string>::post_delayed(ks_apartment::default_mta(), {}, []() -> std::string {
        return "c";
    }, 120);

    ks_future_util::any(f1, f2, f3)
        .on_completion(ks_apartment::default_mta(), {}, [](auto& result) {
            _output_result("completion: ", result);
            g_exit_latch.count_down();
        });

    g_exit_latch.wait();
}

void test_future_methods() {
    g_exit_latch.add(1);
    std::cout << "test future ... ";

    ks_future<std::string>::resolved("a")
        .cast<std::string>()
        .then<std::string>(ks_apartment::default_mta(), {}, [](const std::string& value) {
            std::cout << "->then() ";
            return value + ".then";
        })
        .transform<std::string>(ks_apartment::default_mta(), {}, [](const ks_result<std::string>& result) -> ks_result<std::string> {
            std::cout << "->transform() ";
            return result.is_value() ? ks_result<std::string>(_result_to_str(result) + ".transform") : ks_result<std::string>(result.to_error());
        })
        .flat_then<std::string>(ks_apartment::default_mta(), {}, [](const std::string& value) -> ks_future<std::string> {
            return ks_future<std::string>::resolved(value + ".flat_then");
        })
        .flat_transform<std::string>(ks_apartment::default_mta(), {}, [](const ks_result<std::string>& result) -> ks_future<std::string> {
            return result.is_value() ? ks_future<std::string>::resolved(_result_to_str(result) + ".flat_transform") : ks_future<std::string>::rejected(result.to_error());
        })
        .on_success(ks_apartment::default_mta(), {}, [](const auto& value) -> void {
            std::cout << "->on_success() ";
        })
        .on_failure(ks_apartment::default_mta(), {}, [](const ks_error& error) -> void {
            std::cout << "->on_failure() ";
        })
        .on_completion(ks_apartment::default_mta(), {}, [](const auto& result) -> void {
            std::cout << "->on_completion() ";
            _output_result("completion: ", result);
            g_exit_latch.count_down();
        });

    g_exit_latch.wait();
    g_exit_latch.add(1);

    ks_future<void>::resolved().cast<nothing_t>()
        .cast<void>()
        .then<void>(ks_apartment::default_mta(), {}, []() -> void {
            std::cout << "->then(void) ";
        })
        .transform<void>(ks_apartment::default_mta(), {}, [](const ks_result<void>& result) -> void {
            std::cout << "->transform(void) ";
        })
        .on_failure(ks_apartment::default_mta(), {}, [](const ks_error& error) -> void {
            std::cout << "->on_failure(void) ";
        })
        .on_completion(ks_apartment::default_mta(), {}, [](const ks_result<void>& result) -> void {
            std::cout << "->on_completion(void) ";
            _output_result("complete(void): ", result);
            g_exit_latch.count_down();
        });

    g_exit_latch.wait();
}


//void test_async_task() {
//    g_exit_latch.add(1);
//    std::cout << "test async-task ... ";
//
//    auto a = ks_async_task<nothing_t>(nothing);
//    auto b = ks_async_task<int>(1);
//    auto c = ks_async_task<long, int>(ks_apartment::default_mta(), {}, [](int) -> long {return 2; });
//    auto d = ks_async_task<double, int, long>(ks_apartment::default_mta(), {}, [](int, long) -> double {return 3; });
//    auto e = ks_async_task<std::string, int, long, double>(ks_apartment::default_mta(), {}, [](int, long, double) -> std::string {return "done"; });
//
//    c.connect(b);
//    d.connect(b, c);
//    e.connect(b, c, d);
//
//    e.get_future()
//        .on_completion(ks_apartment::default_mta(), {}, [](auto& result) {
//            _output_result("completion: ", result);
//            g_exit_latch.count_down();
//        });
//
//    g_exit_latch.wait();
//}

void test_alive() {
    g_exit_latch.add(1);
    std::cout << "test alive ... ";

    struct OBJ {
        OBJ(int id) : m_id(id) { ++s_counter(); std::cout << "->::OBJ(id:" << m_id << ") "; }
        ~OBJ() { --s_counter(); std::cout << "->::~OBJ(id:" << m_id << ") "; }
        _DISABLE_COPY_CONSTRUCTOR(OBJ);
        static std::atomic<int>& s_counter() { static std::atomic<int> s_n(0); return s_n; }
        const int m_id;
    };

    if (true) {
        auto obj1 = std::make_shared<OBJ>(1);

        ks_future<void>::post_delayed(
            ks_apartment::default_mta(), ks_async_context(std::move(obj1), nullptr), []() {
                std::cout << "->fn() ";
            }, 100).on_completion(ks_apartment::default_mta(), ks_async_context(), [](auto& result) {
                ks_future<void>::post_delayed(
                    ks_apartment::default_mta(), ks_async_context(), []() {
                        g_exit_latch.count_down(); 
                    }, 100);
            });
    }

    g_exit_latch.wait();
    if (OBJ::s_counter() == 0)
        std::cout << "completion: no leak (succ)\n";
    else
        std::cout << "completion: " << OBJ::s_counter() << " objs leak (error)\n";
}

void test_notification_center() {
    g_exit_latch.add(1);
    std::cout << "test notification-center ... ";

    struct {} sender, observer;
    ks_notification_center::default_center()->add_observer(&observer, "a.b.c.d", ks_apartment::default_mta(), {}, [](const ks_notification& notification) {
        ASSERT(false);
        std::cout << "a.b.c.d notification: name=" << notification.get_notification_name() << "; ";
        });
    ks_notification_center::default_center()->add_observer(&observer, "a.b.*", ks_apartment::default_mta(), {}, [](const ks_notification& notification) {
        std::cout << "a.b.* notification: name=" << notification.get_notification_name() << "; ";
        g_exit_latch.count_down();
        });

    ks_notification_center::default_center()->post_notification(&sender, "a.x.y.z", {}, nothing);
    ks_notification_center::default_center()->post_notification(&sender, "a.b.c", {}, nothing);

    g_exit_latch.wait();
    ks_notification_center::default_center()->remove_observer(&observer, "a.b.c.d");
    ks_notification_center::default_center()->remove_observer(&observer, "a.*");
    ks_notification_center::default_center()->remove_observer(&sender);

    std::cout << "completion (succ)\n";
}


int main() {
    std::cout << "start ...\n";

    test_promise();
    test_post();
    test_post_pending();

    test_all();
    test_any();

    test_future_methods();
    test_post_delayed();

    //test_async_task();
    test_alive();

    test_notification_center();

    g_exit_latch.wait();
    ks_apartment::default_mta()->async_stop();
    ks_apartment::background_sta()->async_stop();
    ks_apartment::default_mta()->wait();
    ks_apartment::background_sta()->wait();
    std::cout << "end.\n";
    return 0;
}