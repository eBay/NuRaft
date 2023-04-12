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
    resp_msg(ulong term,
             msg_type type,
             int32 src,
             int32 dst,
             ulong next_idx = 0L,
             bool accepted = false)
        : msg_base(term, type, src, dst)
        , next_idx_(next_idx)
        , next_batch_size_hint_in_bytes_(0)
        , accepted_(accepted)
        , ctx_(nullptr)
        , cb_func_(nullptr)
        , async_cb_func_(nullptr)
        , result_code_(cmd_result_code::OK)
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
        ctx_ = std::move(src);
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

private:
    ulong next_idx_;
    int64 next_batch_size_hint_in_bytes_;
    bool accepted_;
    ptr<buffer> ctx_;
    ptr<peer> peer_;
    resp_cb cb_func_;
    resp_async_cb async_cb_func_;
    cmd_result_code result_code_;
};

}

#endif //_RESP_MSG_HXX_
