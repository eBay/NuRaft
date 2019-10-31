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

#ifndef _ASYNC_HXX_
#define _ASYNC_HXX_

#include "msg_type.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"

#ifdef _NO_EXCEPTION
#include <cassert>
#endif
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>

namespace nuraft {

enum cmd_result_code {
    OK = 0,
    CANCELLED = -1,
    TIMEOUT = -2,
    NOT_LEADER = -3,
    BAD_REQUEST = -4,
    SERVER_ALREADY_EXISTS = -5,
    CONFIG_CHANGING = -6,
    SERVER_IS_JOINING = -7,
    SERVER_NOT_FOUND = -8,
    CANNOT_REMOVE_LEADER = -9,

    FAILED = -32768,
};

template< typename T,
          typename TE = ptr<std::exception> >
class cmd_result {
public:
    /**
     * This handler will be invoked with the result value only.
     */
    using handler_type = std::function< void(T&, TE&) >;

    /**
     * This handler will be invoked with this instance.
     * User can get more detailed info from this.
     */
    using handler_type2 = std::function< void( cmd_result<T, TE>&, TE& ) >;

    cmd_result()
        : err_()
        , code_(cmd_result_code::OK)
        , has_result_(false)
        , accepted_(false)
        , handler_(nullptr)
        , handler2_(nullptr)
        {}

    explicit cmd_result(T& result)
        : result_(result)
        , err_()
        , code_(cmd_result_code::OK)
        , has_result_(true)
        , accepted_(false)
        , handler_(nullptr)
        , handler2_(nullptr)
        {}

    explicit cmd_result(T& result, bool _accepted)
        : result_(result)
        , err_()
        , code_(cmd_result_code::OK)
        , has_result_(true)
        , accepted_(_accepted)
        , handler_(nullptr)
        , handler2_(nullptr)
        {}

    explicit cmd_result(const handler_type& handler)
        : err_()
        , code_(cmd_result_code::OK)
        , has_result_(true)
        , accepted_(false)
        , handler_(handler)
        , handler2_(nullptr)
        {}

    ~cmd_result() {}

    __nocopy__(cmd_result);

public:

    /**
     * Install a handler that will be invoked when
     * we get the result of replication.
     *
     * @param handler Handler.
     * @return void.
     */
    void when_ready(const handler_type& handler) {
        bool call_handler = false;
        {   std::lock_guard<std::mutex> guard(lock_);
            if (has_result_) call_handler = true;
            else handler_ = handler;
        }
        if (call_handler) handler(result_, err_);
    }

    /**
     * Install a handler (handler_type2) that will be invoked when
     * we get the result of replication.
     *
     * @param handler Handler.
     * @return void.
     */
    void when_ready(const handler_type2& handler) {
        bool call_handler = false;
        {   std::lock_guard<std::mutex> guard(lock_);
            if (has_result_) call_handler = true;
            else handler2_ = handler;
        }
        if (call_handler) handler(*this, err_);
    }

    /**
     * Set result value.
     *
     * @param result Result value.
     * @param err Exception if necessary.
     * @return void.
     */
    void set_result(T& result, TE& err) {
        bool call_handler = false;
        {   std::lock_guard<std::mutex> guard(lock_);
            result_ = result;
            err_ = err;
            has_result_ = true;
            if (handler_ || handler2_) call_handler = true;
        }
        if (call_handler) {
            if (handler2_) handler2_(*this, err);
            else if (handler_) handler_(result, err);
        }
        cv_.notify_all();
    }

    /**
     * Set `accept` flag.
     *
     * @return void.
     */
    void accept() {
        accepted_ = true;
    }

    /**
     * Return `true` if accepted.
     *
     * @return `true` if accepted.
     */
    bool get_accepted() const {
        return accepted_;
    }

    /**
     * Set result code.
     *
     * @param ec Result code.
     * @return void.
     */
    void set_result_code(cmd_result_code ec) {
        std::lock_guard<std::mutex> guard(lock_);
        code_ = ec;
    }

    /**
     * Get the result code of replication.
     *
     * @return Result code.
     */
    cmd_result_code get_result_code() const {
        std::lock_guard<std::mutex> guard(lock_);
        return code_;
    }

    /**
     * Get the result of replication in human-readable form.
     *
     * @return Result string.
     */
    std::string get_result_str() const {
        cmd_result_code code = get_result_code();

        static std::unordered_map<int, std::string>
            code_str_map
            ( { {cmd_result_code::OK,
                 "Ok."},
                {cmd_result_code::CANCELLED,
                 "Request cancelled."},
                {cmd_result_code::TIMEOUT,
                 "Request timeout."},
                {cmd_result_code::NOT_LEADER,
                 "This node is not a leader."},
                {cmd_result_code::BAD_REQUEST,
                 "Invalid request."},
                {cmd_result_code::SERVER_ALREADY_EXISTS,
                 "Server already exists in the cluster."},
                {cmd_result_code::CONFIG_CHANGING,
                 "Previous configuration change has not been committed yet."},
                {cmd_result_code::SERVER_IS_JOINING,
                 "Other server is being added."},
                {cmd_result_code::SERVER_NOT_FOUND,
                 "Cannot find server."},
                {cmd_result_code::CANNOT_REMOVE_LEADER,
                 "Cannot remove leader."},
                {cmd_result_code::FAILED,
                 "Failed."}
            } );
        auto entry = code_str_map.find((int)code);
        if (entry == code_str_map.end()) {
            return "Unknown (" + std::to_string((int)code) + ").";
        }
        return entry->second;
    }

    /**
     * Get the result value.
     *
     * @return Result value.
     */
    T& get() {
        std::unique_lock<std::mutex> lock(lock_);
        if (has_result_) {
            if (err_ == nullptr) {
                return result_;
            }
            // Return empty result rather than throw exception.
            // Caller should handle it properly.
            return empty_result_;
        }

        cv_.wait(lock);
        if (err_ == nullptr) {
            return result_;
        }

        return empty_result_;
    }

private:
    T empty_result_;
    T result_;
    TE err_;
    cmd_result_code code_;
    bool has_result_;
    bool accepted_;
    handler_type handler_;
    handler_type2 handler2_;
    mutable std::mutex lock_;
    std::condition_variable cv_;
};

// For backward compatibility.
template< typename T,
          typename TE = ptr<std::exception> >
using async_result = cmd_result<T, TE>;

}

#endif //_ASYNC_HXX_
