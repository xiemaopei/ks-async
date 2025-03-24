﻿/* Copyright 2024 The Kingsoft's ks-async/ktl Authors. All Rights Reserved.

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

//文件说明：ks_cxxbase.h是最基础的头文件，所有其他ktl头文件都应先include此文件
#pragma once


//基本数据类型定义
#include <cstddef>
#include <cstdint>
#include <type_traits>

using byte = uint8_t;
using uint = unsigned int;
using nullptr_t = decltype(nullptr);
using HRESULT = std::conditional<sizeof(long) == 4, long, int32_t>::type;

#ifndef __NOTHING_DEF
#define __NOTHING_DEF
struct nothing_t {};
constexpr nothing_t nothing = {};  //nothing相当于其他现代语言中的unit，但unit这个名字与uint太容易混淆了，所以我们用nothing来命名
#endif //__NOTHING_DEF


//宏_ABSTRACT定义
#ifndef _ABSTRACT
#	define _ABSTRACT
#endif

//宏_AS_NAMESPACE定义
#ifndef _AS_NAMESPACE
#	define _AS_NAMESPACE
#endif

//宏_DISABLE_COPY_CONSTRUCTOR定义
#ifndef _DISABLE_COPY_CONSTRUCTOR
#	define _DISABLE_COPY_CONSTRUCTOR(ThisClass)            \
			ThisClass(const ThisClass&) = delete;          \
			ThisClass& operator=(const ThisClass&) = delete;
#endif

//宏_DECL_DEPRECATED定义
#ifndef _DECL_DEPRECATED
#	if defined(_MSC_VER)
#		define _DECL_DEPRECATED __declspec(deprecated)
#	elif defined(__GNUC__)
#		define _DECL_DEPRECATED __attribute__((__deprecated__))
#	else
#		define _DECL_DEPRECATED
#	endif
#endif

//宏_DECL_EXPORT定义
#ifndef _DECL_EXPORT
#	if defined(_MSC_VER)
#		define _DECL_EXPORT __declspec(dllexport)
#	elif defined(__GNUC__)
#		define _DECL_EXPORT __attribute__((visibility("default")))
#	else
#		error how to decl-export?
#	endif
#endif

//宏_DECL_IMPORT定义
#ifndef _DECL_IMPORT
#	if defined(_MSC_VER)
#		define _DECL_IMPORT __declspec(dllimport)
#	elif defined(__GNUC__)
#		define _DECL_IMPORT __attribute__((visibility("default")))
#	else
#		error how to decl-import?
#	endif
#endif

//宏_NODISCARD定义
#ifndef _NODISCARD
#	if defined(_MSC_VER)
#		define _NODISCARD _Check_return_
#	else
#		define _NODISCARD __attribute__((warn_unused_result))
#	endif
#endif

//宏_NOOP定义
#ifndef _NOOP
#	define _NOOP(...)  ((void)(0))
#endif

//宏_UNUSED定义
#ifndef _UNUSED
#	define _UNUSED(x)  ((void)(x))
#endif

//宏_DEBUG定义
#ifndef _DEBUG
#	if !defined(NDEBUG)
#		define _DEBUG
#	endif
#endif

//ASSERT宏定义
#ifndef ASSERT
#	ifdef _DEBUG
#		if defined(_MSC_VER)
#			include <crtdbg.h>
#			define ASSERT _ASSERT
#		else
#			include <assert.h>
#			define ASSERT assert
#		endif
#	else
#		define ASSERT(x)  ((void)(0))
#	endif
#endif
