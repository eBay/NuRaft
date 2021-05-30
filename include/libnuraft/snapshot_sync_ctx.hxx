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

#ifndef _SNAPSHOT_SYNC_CTX_HXX_
#define _SNAPSHOT_SYNC_CTX_HXX_

#include "internal_timer.hxx"

namespace nuraft {

class snapshot;
struct snapshot_sync_ctx {
public:
    snapshot_sync_ctx(const ptr<snapshot>& s,
                      ulong timeout_ms,
                      ulong offset = 0L)
        : snapshot_(s)
        , offset_(offset)
        , user_snp_ctx_(nullptr)
    {
        // 10 seconds by default.
        timer_.set_duration_ms(timeout_ms);
    }

    __nocopy__(snapshot_sync_ctx);

public:
    const ptr<snapshot>& get_snapshot() const { return snapshot_; }
    ulong get_offset() const { return offset_; }
    ulong get_obj_idx() const { return obj_idx_; }
    void*& get_user_snp_ctx() { return user_snp_ctx_; }

    void set_offset(ulong offset) {
        if (offset_ != offset) timer_.reset();
        offset_ = offset;
    }
    void set_obj_idx(ulong obj_idx) { obj_idx_ = obj_idx; }
    void set_user_snp_ctx(void* _user_snp_ctx) { user_snp_ctx_ = _user_snp_ctx; }

    timer_helper& get_timer() { return timer_; }

public:
    ptr<snapshot> snapshot_;
    // Can be used for either byte offset or object index.
    union {
        ulong offset_;
        ulong obj_idx_;
    };
    void* user_snp_ctx_;
    timer_helper timer_;
};

}

#endif //_SNAPSHOT_SYNC_CTX_HXX_
