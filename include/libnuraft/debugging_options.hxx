#pragma once

#include <atomic>
#include <cstdlib>
#include <cstddef>

namespace nuraft {

struct debugging_options {
    debugging_options()
        : disable_reconn_backoff_(false)
        , handle_cli_req_sleep_us_(0)
        {}

    static debugging_options& get_instance() {
        static debugging_options opt;
        return opt;
    }

    /**
     * If `true`, reconnection back-off timer will be disabled,
     * and there will be frequent reconnection attempts for every
     * request to follower.
     */
    std::atomic<bool> disable_reconn_backoff_;

    /**
     * If non-zero, the thread will sleep the given amount of time
     * inside `handle_cli_req` function.
     */
    std::atomic<size_t> handle_cli_req_sleep_us_;
};

}
