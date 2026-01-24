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
using resp_cb =
    std::function<ptr<resp_msg>(ptr<resp_msg>)>;

using resp_async_cb =
    std::function< ptr< cmd_result< ptr<buffer> > >() >;

class resp_msg : public msg_base {
public:
    // If set, the follower is marked down itself,
    // usually when the follower is in the middle of termination.
    // This is the hint for the leader.
    static constexpr uint64_t SELF_MARK_DOWN = 0x1;

    resp_msg(ulong term,
             msg_type type,
             int32 src,
             int32 dst,
             ulong next_idx = 0L,
             bool accepted = false,
             int32 group_id = 0)
        : msg_base(term, type, src, dst)
        , next_idx_(next_idx)
        , next_batch_size_hint_in_bytes_(0)
        , accepted_(accepted)
        , group_id_(group_id)
        , ctx_(nullptr)
        , cb_func_(nullptr)
        , async_cb_func_(nullptr)
        , result_code_(cmd_result_code::OK)
        , extra_flags_(0x0)
        {}

    __nocopy__(resp_msg);

public:
    ulong get_next_idx() const {
        return next_idx_;
    }

    int64 get_next_batch_size_hint_in_bytes() const {
        return next_batch_size_hint_in_bytes_;
    }

    void set_next_batch_size_hint_in_bytes(int64 bytes) {
        next_batch_size_hint_in_bytes_ = bytes;
    }

    bool get_accepted() const {
        return accepted_;
    }

    void accept(ulong next_idx) {
        next_idx_ = next_idx;
        accepted_ = true;
    }

    void set_ctx(ptr<buffer> src) {
        ctx_ = src;
    }

    ptr<buffer> get_ctx() const {
        return ctx_;
    }

    void set_peer(ptr<peer> peer) {
        peer_ = peer;
    }

    ptr<peer> get_peer() const {
        return peer_;
    }

    void set_cb(resp_cb _func) {
        cb_func_ = _func;
    }

    bool has_cb() const {
        if (cb_func_) return true;
        return false;
    }

    ptr<resp_msg> call_cb(ptr<resp_msg>& _resp) {
        return cb_func_(_resp);
    }

    void set_async_cb(resp_async_cb _func) {
        async_cb_func_ = _func;
    }

    bool has_async_cb() const {
        if (async_cb_func_) return true;
        return false;
    }

    ptr< cmd_result< ptr<buffer> > > call_async_cb() {
        return async_cb_func_();
    }

    void set_result_code(cmd_result_code rc) {
        result_code_ = rc;
    }

    cmd_result_code get_result_code() const {
        return result_code_;
    }

    void set_extra_flags(uint64_t flags) {
        extra_flags_ = flags;
    }

    uint64_t get_extra_flags() const {
        return extra_flags_;
    }

    int32 get_group_id() const {
        return group_id_;
    }

    void set_group_id(int32 id) {
        group_id_ = id;
    }

private:
    ulong next_idx_;

    // Hint for the leader about the next batch size (only when non-zero).
    int64 next_batch_size_hint_in_bytes_;
    bool accepted_;

    // Group identifier for port sharing feature.
    // Default 0 for backward compatibility.
    int32 group_id_;

    ptr<buffer> ctx_;
    ptr<peer> peer_;
    resp_cb cb_func_;
    resp_async_cb async_cb_func_;
    cmd_result_code result_code_;
    uint64_t extra_flags_;
};

}

#endif //_RESP_MSG_HXX_
