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
#include "ks_notification.h"
#include "ks_apartment.h"
#include <memory>


class ks_notification_center {
public:
	KS_ASYNC_API explicit ks_notification_center(const char* center_name);
	_DISABLE_COPY_CONSTRUCTOR(ks_notification_center);

	KS_ASYNC_API static ks_notification_center* default_center(); //default-center singleton

public:
	KS_ASYNC_API const char* name();

public:
	KS_ASYNC_API void add_observer(
		const void* observer, const char* notification_name_pattern, 
		ks_apartment* apartment, std::function<void(const ks_notification&)> fn, const ks_async_context& context = {});

	KS_ASYNC_API void remove_observer(const void* observer, const char* notification_name_pattern);

	KS_ASYNC_API void remove_observer(const void* observer);

public:
	KS_ASYNC_API void post_notification(const ks_notification& notification);

	template <class DATA_TYPE, class X = DATA_TYPE>
	KS_ASYNC_INLINE_API void post_notification(const void* sender, const char* notification_name, X&& notification_data, const ks_async_context& notification_context = {}) {
		return this->post_notification(ks_notification().set_sender(sender).set_notification_name(notification_name).set_notification_data<DATA_TYPE>(notification_data).set_notification_context(notification_context));
	}

	KS_ASYNC_INLINE_API void post_simple_notification(const void* sender, const char* notification_name, const ks_async_context& notification_context = {}) {
		return this->post_notification(ks_notification().set_sender(sender).set_notification_name(notification_name).set_notification_context(notification_context));
	}

private:
	class __ks_notification_center_data;
	std::shared_ptr<__ks_notification_center_data> m_d;
};
