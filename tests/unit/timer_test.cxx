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

}  // namespace timer_test;
using namespace timer_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = false;

    ts.doTest( "timer basic test",
               timer_basic_test );

    ts.doTest( "timer cancel test",
               timer_cancel_test );

    return 0;
}


