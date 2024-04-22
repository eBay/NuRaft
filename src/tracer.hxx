/************************************************************************
Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include "fix_format.hxx"
#include "logger.hxx"

#include <string>

#include <stdarg.h>

#ifdef _WIN32
static inline std::string msg_if_given(_Printf_format_string_ const char* format, ...) {
#else
static inline std::string msg_if_given(const char* format, ...)
    __attribute__((format(printf, 1, 2)));

static inline std::string msg_if_given(const char* format, ...) {
#endif
    if (format[0] == 0x0) {
        return "";
    }
    _fix_format(format);
    std::string msg(2048, '\0');
    for (int i = 0; i < 2; ++i) {
        va_list args;
        va_start(args, format);
        const int len = vsnprintf(&msg[0], msg.size(), format, args);
        va_end(args);
        if (len < 0) {
            return std::string("invalid format ") + format;
        }
        if (static_cast<size_t>(len) < msg.size()) {
            msg.resize(len);
            break;
        }
        msg.resize(len + 1);
    }

    // Get rid of newline at the end.
    if ((not msg.empty()) && (msg.back() == '\n')) {
        msg.pop_back();
    }
    return msg;
}

#define L_TRACE (6)
#define L_DEBUG (5)
#define L_INFO  (4)
#define L_WARN  (3)
#define L_ERROR (2)
#define L_FATAL (1)

#define p_lv(lv, ...) \
    if (l_ && l_->get_level() >= (lv)) \
        l_->put_details((lv), __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// trace.
#define p_tr(...) \
    if (l_ && l_->get_level() >= 6) \
        l_->put_details(6, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// debug verbose.
#define p_dv(...) \
    if (l_ && l_->get_level() >= 5) \
        l_->put_details(5, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// debug.
#define p_db(...) \
    if (l_ && l_->get_level() >= 5) \
        l_->put_details(5, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// info.
#define p_in(...) \
    if (l_ && l_->get_level() >= 4) \
        l_->put_details(4, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// warning.
#define p_wn(...) \
    if (l_ && l_->get_level() >= 3) \
        l_->put_details(3, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// error.
#define p_er(...) \
    if (l_ && l_->get_level() >= 2) \
        l_->put_details(2, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

// fatal.
#define p_ft(...) \
    if (l_ && l_->get_level() >= 1) \
        l_->put_details(1, __FILE__, __func__, __LINE__, msg_if_given(__VA_ARGS__))

