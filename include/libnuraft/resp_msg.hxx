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

#ifndef _RESP_MSG_HXX_
#define _RESP_MSG_HXX_

#include "async.hxx"
#include "buffer.hxx"
#include "msg_base.hxx"

namespace nuraft {

class resp_msg;
class peer;
using resp_cb = std::function< std::shared_ptr< resp_msg >(std::shared_ptr< resp_msg >) >;

using resp_async_cb = std::function< std::shared_ptr< cmd_result< std::shared_ptr< buffer > > >() >;

class resp_msg : public msg_base {
public:
    resp_msg(uint64_t term, msg_type type, int32_t src, int32_t dst, uint64_t next_idx = 0L, bool accepted = false) :
            msg_base(term, type, src, dst),
            next_idx_(next_idx),
            next_batch_size_hint_in_bytes_(0),
            accepted_(accepted),
            ctx_(nullptr),
            cb_func_(nullptr),
            async_cb_func_(nullptr),
            result_code_(cmd_result_code::OK) {}

    __nocopy__(resp_msg);

public:
    uint64_t get_next_idx() const { return next_idx_; }

    int64_t get_next_batch_size_hint_in_bytes() const { return next_batch_size_hint_in_bytes_; }

    void set_next_batch_size_hint_in_bytes(int64_t bytes) { next_batch_size_hint_in_bytes_ = bytes; }

    bool get_accepted() const { return accepted_; }

    void accept(uint64_t next_idx) {
        next_idx_ = next_idx;
        accepted_ = true;
    }

    void set_ctx(std::shared_ptr< buffer > src) { ctx_ = src; }

    std::shared_ptr< buffer > get_ctx() const { return ctx_; }

    void set_peer(std::shared_ptr< peer > peer) { peer_ = peer; }

    std::shared_ptr< peer > get_peer() const { return peer_; }

    void set_cb(resp_cb _func) { cb_func_ = _func; }

    bool has_cb() const {
        if (cb_func_) return true;
        return false;
    }

    std::shared_ptr< resp_msg > call_cb(std::shared_ptr< resp_msg >& _resp) { return cb_func_(_resp); }

    void set_async_cb(resp_async_cb _func) { async_cb_func_ = _func; }

    bool has_async_cb() const {
        if (async_cb_func_) return true;
        return false;
    }

    std::shared_ptr< cmd_result< std::shared_ptr< buffer > > > call_async_cb() { return async_cb_func_(); }

    void set_result_code(cmd_result_code rc) { result_code_ = rc; }

    cmd_result_code get_result_code() const { return result_code_; }

private:
    uint64_t next_idx_;
    int64_t next_batch_size_hint_in_bytes_;
    bool accepted_;
    std::shared_ptr< buffer > ctx_;
    std::shared_ptr< peer > peer_;
    resp_cb cb_func_;
    resp_async_cb async_cb_func_;
    cmd_result_code result_code_;
};

} // namespace nuraft

#endif //_RESP_MSG_HXX_
