/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

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

#include <cstdint>
#include <mutex>

#define __nocopy__(clazz)                                                                                              \
private:                                                                                                               \
    clazz(const clazz&) = delete;                                                                                      \
    clazz& operator=(const clazz&) = delete

#define __interface_body__(clazz)                                                                                      \
public:                                                                                                                \
    clazz() = default;                                                                                                 \
    virtual ~clazz() = default;                                                                                        \
    __nocopy__(clazz)

using auto_lock = std::lock_guard< std::mutex >;
using recur_lock = std::unique_lock< std::recursive_mutex >;

auto constexpr sz_int = sizeof(int32_t);
auto constexpr sz_uint64_t = sizeof(uint64_t);
auto constexpr sz_byte = sizeof(std::byte);
