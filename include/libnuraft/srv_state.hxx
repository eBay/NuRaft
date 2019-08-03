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

#ifndef _SRV_STATE_HXX_
#define _SRV_STATE_HXX_

#include "basic_types.hxx"

#include <atomic>

namespace nuraft {

class srv_state {
public:
    srv_state()
        : term_(0L)
        , voted_for_(-1)
        {}

    srv_state(ulong term, int voted_for)
        : term_(term)
        , voted_for_(voted_for)
        {}

    __nocopy__(srv_state);

public:
    static ptr<srv_state> deserialize(buffer& buf) {
        ulong term = buf.get_ulong();
        int voted_for = buf.get_int();
        return cs_new<srv_state>(term, voted_for);
    }

    ulong   get_term() const        { return term_; }
    void    set_term(ulong term)    { term_ = term; }
    void    inc_term()              { term_++; }

    int     get_voted_for() const           { return voted_for_; }
    void    set_voted_for(int voted_for)    { voted_for_ = voted_for; }

    ptr<buffer> serialize() const {
        ptr<buffer> buf = buffer::alloc(sz_ulong + sz_int);
        buf->put(term_);
        buf->put(voted_for_);
        buf->pos(0);
        return buf;
    }

private:
    std::atomic<ulong> term_;
    std::atomic<int> voted_for_;
};

}

#endif
