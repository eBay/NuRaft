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

#include "nuraft.hxx"

#include "test_common.h"

#include <atomic>
#include <sys/time.h>

using namespace nuraft;

namespace timer_test {

void timer_invoke_handler(std::atomic<size_t>* counter) {
    if (counter) (*counter)++;
}

int timer_basic_test() {
    asio_service svc;
    std::atomic<size_t> counter(0);
    timer_task<void>::executor handler = std::bind( timer_invoke_handler,
                                                    &counter );
    ptr<delayed_task> task = cs_new< timer_task<void> >( handler );

    // Set 200 ms timer and wait 300 ms.
    svc.schedule(task, 200);
    TestSuite::sleep_ms(300);

    // It should be invoked only once.
    CHK_EQ(1, counter);

    svc.stop();
    size_t count = 0;
    while (svc.get_active_workers() && count < 100) {
        // 10ms per tick.
        TestSuite::sleep_ms(10);
        count++;
    }

    return 0;
}

int timer_cancel_test() {
    asio_service svc;
    std::atomic<size_t> counter(0);
    timer_task<void>::executor handler = std::bind( timer_invoke_handler,
                                                    &counter );
    ptr<delayed_task> task = cs_new< timer_task<void> >( handler );

    // Set 300 ms timer, wait 100 ms, and then cancel it.
    svc.schedule(task, 300);
    TestSuite::sleep_ms(100);
    svc.cancel(task);
    TestSuite::sleep_ms(300);

    // It should never be invoked.
    CHK_EQ(0, counter);

    // Re-schedule it.
    svc.schedule(task, 300);
    TestSuite::sleep_ms(500);

    // It should be invoked only once.
    CHK_EQ(1, counter);

    svc.stop();
    size_t count = 0;
    while (svc.get_active_workers() && count < 100) {
        // 10ms per tick.
        TestSuite::sleep_ms(10);
        count++;
    }

    return 0;
}

int internal_timer_test() {
    // Get timestamp from `gettimeofday`.
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    uint64_t start_time_us = tv.tv_sec * 1000000 + tv.tv_usec;

    timer_helper::sleep_ms(1);

    // Get timestamp from `timer_helper`.
    uint64_t middle_time_us = timer_helper::get_timeofday_us();

    timer_helper::sleep_ms(1);

    // Get timestamp from `gettimeofday` again.
    gettimeofday(&tv, nullptr);
    uint64_t end_time_us = tv.tv_sec * 1000000 + tv.tv_usec;

    TestSuite::Msg mm;
    mm << "start_time_us: " << start_time_us << std::endl
       << "middle_time_us: " << middle_time_us << std::endl
       << "end_time_us: " << end_time_us << std::endl;

    CHK_GT(middle_time_us, start_time_us);
    CHK_GT(end_time_us, middle_time_us);
    return 0;
}


static std::atomic<int64_t> test_clock_us(0);

static timer_helper::time_point test_clock() {
    return timer_helper::time_point( std::chrono::microseconds( test_clock_us.load() ) );
}

int internal_timer_custom_clock_test() {
    test_clock_us = 1000000;
    timer_helper::set_clock(&test_clock);

    timer_helper tt(100 * 1000);
    CHK_EQ(0, tt.get_us());
    CHK_FALSE(tt.timeout());

    // Real time passing has no effect on a custom clock.
    timer_helper::sleep_ms(5);
    CHK_EQ(0, tt.get_us());
    CHK_FALSE(tt.timeout());

    test_clock_us += 100 * 1000;
    CHK_EQ(100, tt.get_ms());
    CHK_FALSE(tt.timeout());
    CHK_FALSE(tt.timeout_and_reset());

    test_clock_us += 1;
    CHK_TRUE(tt.timeout());
    CHK_TRUE(tt.timeout_and_reset());
    // Reset to the custom clock's current time.
    CHK_EQ(0, tt.get_us());
    CHK_FALSE(tt.timeout());

    test_clock_us += 2500 * 1000;
    CHK_EQ(2, tt.get_sec());
    CHK_EQ(2500, tt.get_ms());

    // `reset()` with an offset is relative to the custom clock.
    tt.reset(-50);
    CHK_EQ(50, tt.get_ms());

    // `fire_first_event` still fires immediately.
    timer_helper first(1000 * 1000, true);
    CHK_TRUE(first.timeout());
    CHK_FALSE(first.timeout());

    // Restore the real clock.
    timer_helper::set_clock(nullptr);
    timer_helper real_tt(1000);
    timer_helper::sleep_ms(5);
    CHK_TRUE(real_tt.timeout());
    return 0;
}

}  // namespace timer_test;
using namespace timer_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = false;

    ts.doTest( "timer basic test",
               timer_basic_test );

    ts.doTest( "timer cancel test",
               timer_cancel_test );

    ts.doTest( "internal timer test",
               internal_timer_test );

    ts.doTest( "internal timer custom clock test",
               internal_timer_custom_clock_test );

    return 0;
}


