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

#ifndef _PP_UTIL_HXX_
#define _PP_UTIL_HXX_

#define __override__ override

#define __nocopy__(clazz)                       \
    private:                                    \
    clazz(const clazz&) = delete;               \
    clazz& operator=(const clazz&) = delete;    \

#define nilptr nullptr

#define __interface_body__(clazz)   \
    public:                         \
    clazz(){}                       \
    virtual ~clazz() {}             \
    __nocopy__(clazz)

#define auto_lock(lock)     std::lock_guard<std::mutex> guard(lock)
#define recur_lock(lock)    std::unique_lock<std::recursive_mutex> guard(lock)

#define sz_int      sizeof(int32)
#define sz_ulong    sizeof(ulong)
#define sz_byte     sizeof(byte)

#endif //_PP_UTIL_HXX_

