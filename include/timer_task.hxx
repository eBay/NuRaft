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

#ifndef _TIMER_TASK_HXX_
#define _TIMER_TASK_HXX_

#include "delayed_task.hxx"

namespace nuraft {

enum timer_task_type {
    election_timer = 0x1,
    heartbeat_timer = 0x2,
};

template<typename T>
class timer_task : public delayed_task {
public:
    typedef std::function<void(T)> executor;

    timer_task(executor& e, T ctx, int32 type = 0)
        : delayed_task(type), exec_(e), ctx_(ctx) {}
protected:
    virtual void exec() __override__ {
        if (exec_) {
            exec_(ctx_);
        }
    }
private:
    executor exec_;
    T ctx_;
};

template<>
class timer_task<void> : public delayed_task {
public:
    typedef std::function<void()> executor;

    explicit timer_task(executor& e, int32 type = 0)
        : delayed_task(type)
        , exec_(e) {}
protected:
    virtual void exec() __override__ {
        if (exec_) {
            exec_();
        }
    }
private:
    executor exec_;
};

}

#endif //_TIMER_TASK_HXX_
