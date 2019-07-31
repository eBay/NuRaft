/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/greensky00/event_awaiter
         (v0.1.1)

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

#include <atomic>
#include <condition_variable>
#include <mutex>

class EventAwaiter {
private:
    enum class AS {
        idle    = 0x0,
        ready   = 0x1,
        waiting = 0x2,
        done    = 0x3
    };

public:
    EventAwaiter() : status(AS::idle) {}

    void reset() {
        status.store(AS::idle);
    }

    void wait() {
        wait_us(0);
    }

    void wait_ms(size_t time_ms) {
        wait_us(time_ms * 1000);
    }

    void wait_us(size_t time_us) {
        AS expected = AS::idle;
        if (status.compare_exchange_strong(expected, AS::ready)) {
            // invoke() has not been invoked yet, wait for it.
            std::unique_lock<std::mutex> l(cvLock);
            expected = AS::ready;
            if (status.compare_exchange_strong(expected, AS::waiting)) {
                if (time_us) {
                    cv.wait_for(l, std::chrono::microseconds(time_us));
                } else {
                    cv.wait(l);
                }
                status.store(AS::done);
            } else {
                // invoke() has grabbed `cvLock` earlier than this.
            }
        } else {
            // invoke() already has been called earlier than this.
        }
    }

    void invoke() {
        AS expected = AS::idle;
        if (status.compare_exchange_strong(expected, AS::done)) {
            // wait() has not been invoked yet, do nothing.
            return;
        }

        std::unique_lock<std::mutex> l(cvLock);
        expected = AS::ready;
        if (status.compare_exchange_strong(expected, AS::done)) {
            // wait() has been called earlier than invoke(),
            // but invoke() has grabbed `cvLock` earlier than wait().
            // Do nothing.
        } else {
            // wait() is waiting for ack.
            cv.notify_all();
        }
    }

private:
    std::atomic<AS> status;
    std::mutex cvLock;
    std::condition_variable cv;
};


