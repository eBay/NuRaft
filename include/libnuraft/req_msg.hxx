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
    req_msg(uint64_t term, msg_type type, int32_t src, int32_t dst, uint64_t last_log_term, uint64_t last_log_idx,
            uint64_t commit_idx) :
            msg_base(term, type, src, dst),
            last_log_term_(last_log_term),
            last_log_idx_(last_log_idx),
            commit_idx_(commit_idx),
            log_entries_() {}

    ~req_msg() override {}

    __nocopy__(req_msg);

public:
    uint64_t get_last_log_idx() const { return last_log_idx_; }

    uint64_t get_last_log_term() const { return last_log_term_; }

    uint64_t get_commit_idx() const { return commit_idx_; }

    std::vector< std::shared_ptr< log_entry > >& log_entries() { return log_entries_; }

private:
    // Term of last log below.
    uint64_t last_log_term_;

    // Last log index that the destination (i.e., follower) node
    // currently has. If below `log_entries_` contains logs,
    // the starting index will be `last_log_idx_ + 1`.
    uint64_t last_log_idx_;

    // Source (i.e., leader) node's current committed log index.
    // As a pipelining, follower will do commit on this index number
    // after appending given logs.
    uint64_t commit_idx_;

    // Logs. Can be empty.
    std::vector< std::shared_ptr< log_entry > > log_entries_;
};

} // namespace nuraft

#endif //_REG_MSG_HXX_
