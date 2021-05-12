#pragma once

#include <atomic>

namespace nuraft {

struct debugging_options {
    debugging_options()
        : disable_reconn_backoff_(false)
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
};

}
