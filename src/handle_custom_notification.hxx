/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.

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

#pragma once

#include "buffer.hxx"
#include "ptr.hxx"

namespace nuraft {

class custom_notification_msg {
public:
    enum type {
        out_of_log_range_warning    = 1,
        leadership_takeover         = 2,
        request_resignation         = 3,
    };

    custom_notification_msg(type t = out_of_log_range_warning)
        : type_(t)
        , ctx_(nullptr)
        {}

    static ptr<custom_notification_msg> deserialize(buffer& buf);

    ptr<buffer> serialize() const;

    type type_;

    ptr<buffer> ctx_;
};

class out_of_log_msg {
public:
    out_of_log_msg()
        : start_idx_of_leader_(0)
        {}

    static ptr<out_of_log_msg> deserialize(buffer& buf);

    ptr<buffer> serialize() const;

    ulong start_idx_of_leader_;
};

class force_vote_msg {
public:
    force_vote_msg() {}

    static ptr<force_vote_msg> deserialize(buffer& buf);

    ptr<buffer> serialize() const;
};

} // namespace nuraft;

