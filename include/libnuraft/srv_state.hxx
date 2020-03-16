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
#include "buffer_serializer.hxx"

#include <atomic>
#include <cassert>
#include <functional>

namespace nuraft {

class srv_state {
public:
    srv_state()
        : term_(0L)
        , voted_for_(-1)
        , election_timer_allowed_(true)
        {}

    srv_state(ulong term, int voted_for, bool et_allowed)
        : term_(term)
        , voted_for_(voted_for)
        , election_timer_allowed_(et_allowed)
        {}

    /**
     * Callback function type for increasing term.
     *
     * @param Current term.
     * @return New term, it should be greater than current term.
     */
    using inc_term_func = std::function< ulong(ulong) >;

    __nocopy__(srv_state);

public:
    static ptr<srv_state> deserialize(buffer& buf) {
        if (buf.size() > sz_ulong + sz_int) {
            return deserialize_v1p(buf);
        }
        // Backward compatibility.
        return deserialize_v0(buf);
    }

    static ptr<srv_state> deserialize_v0(buffer& buf) {
        ulong term = buf.get_ulong();
        int voted_for = buf.get_int();
        return cs_new<srv_state>(term, voted_for, true);
    }

    static ptr<srv_state> deserialize_v1p(buffer& buf) {
        buffer_serializer bs(buf);
        uint8_t ver = bs.get_u8();
        (void)ver;
        ulong term = bs.get_u64();
        int voted_for = bs.get_i32();
        bool et_allowed = (bs.get_u8() == 1);
        return cs_new<srv_state>(term, voted_for, et_allowed);
    }

    void set_inc_term_func(inc_term_func to) {
        inc_term_cb_ = to;
    }

    ulong get_term() const {
        return term_;
    }

    void set_term(ulong term) {
        term_ = term;
    }

    void inc_term() {
        if (inc_term_cb_) {
            ulong new_term = inc_term_cb_(term_);
            assert(new_term > term_);
            term_ = new_term;
            return;
        }
        term_++;
    }

    int get_voted_for() const {
        return voted_for_;
    }

    void set_voted_for(int voted_for) {
        voted_for_ = voted_for;
    }

    bool is_election_timer_allowed() const {
        return election_timer_allowed_;
    }

    void allow_election_timer(bool to) {
        election_timer_allowed_ = to;
    }

    ptr<buffer> serialize() const {
        return serialize_v1p(CURRENT_VERSION);
    }

    ptr<buffer> serialize_v0() const {
        ptr<buffer> buf = buffer::alloc(sz_ulong + sz_int);
        buf->put(term_);
        buf->put(voted_for_);
        buf->pos(0);
        return buf;
    }

    ptr<buffer> serialize_v1p(size_t version) const {
        //   << Format >>
        // version          1 byte
        // term             8 bytes
        // voted_for        4 bytes
        // election timer   1 byte
        ptr<buffer> buf = buffer::alloc( sizeof(uint8_t) +
                                         sizeof(uint64_t) +
                                         sizeof(int32_t) +
                                         sizeof(uint8_t) );
        buffer_serializer bs(buf);
        bs.put_u8(version);
        bs.put_u64(term_);
        bs.put_i32(voted_for_);
        bs.put_u8( election_timer_allowed_ ? 1 : 0 );
        return buf;
    }

private:
    const uint8_t CURRENT_VERSION = 1;

    /**
     * Term.
     */
    std::atomic<ulong> term_;

    /**
     * Server ID that this server voted for.
     * `-1` if not voted.
     */
    std::atomic<int> voted_for_;

    /**
     * `true` if election timer is allowed.
     */
    std::atomic<bool> election_timer_allowed_;

    /**
     * Custom callback function for increasing term.
     * If not given, term will be increased by 1.
     */
    std::function< ulong(ulong) > inc_term_cb_;
};

}

#endif
