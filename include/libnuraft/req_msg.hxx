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

#ifndef _REG_MSG_HXX_
#define _REG_MSG_HXX_

#include "log_entry.hxx"
#include "msg_base.hxx"

#include <vector>

namespace nuraft {

class req_msg : public msg_base {
public:
    // If set, the receiver of this request is not in the quorum.
    // This flag is used only when full consensus mode is enabled.
    static constexpr uint64_t EXCLUDED_FROM_THE_QUORUM = 0x1;

    req_msg(ulong term,
            msg_type type,
            int32 src,
            int32 dst,
            ulong last_log_term,
            ulong last_log_idx,
            ulong commit_idx)
        : msg_base(term, type, src, dst)
        , last_log_term_(last_log_term)
        , last_log_idx_(last_log_idx)
        , commit_idx_(commit_idx)
        , log_entries_()
        , extra_flags_(0)
        { }

    virtual ~req_msg() __override__ { }

    __nocopy__(req_msg);

public:
    ulong get_last_log_idx() const {
        return last_log_idx_;
    }

    ulong get_last_log_term() const {
        return last_log_term_;
    }

    ulong get_commit_idx() const {
        return commit_idx_;
    }

    std::vector<ptr<log_entry>>& log_entries() {
        return log_entries_;
    }

    void set_extra_flags(uint64_t flags) {
        extra_flags_ = flags;
    }

    uint64_t get_extra_flags() const {
        return extra_flags_;
    }

private:
    // Term of last log below.
    ulong last_log_term_;

    // Last log index that the destination (i.e., follower) node
    // currently has. If below `log_entries_` contains logs,
    // the starting index will be `last_log_idx_ + 1`.
    ulong last_log_idx_;

    // Source (i.e., leader) node's current committed log index.
    // As a pipelining, follower will do commit on this index number
    // after appending given logs.
    ulong commit_idx_;

    // Logs. Can be empty.
    std::vector<ptr<log_entry>> log_entries_;

    // Optional extra flags to pass additional information.
    uint64_t extra_flags_;
};

}

#endif //_REG_MSG_HXX_
