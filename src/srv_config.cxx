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

#include "srv_config.hxx"

namespace nuraft {

static const uint8_t LEARNER_FLAG = 0x1;
static const uint8_t NEW_JOINER_FLAG = 0x2;

ptr<srv_config> srv_config::deserialize(buffer& buf) {
    buffer_serializer bs(buf);
    return deserialize(bs);
}

ptr<srv_config> srv_config::deserialize(buffer_serializer& bs) {
    int32 id = bs.get_i32();
    int32 dc_id = bs.get_i32();
    const char* endpoint_char = bs.get_cstr();
    const char* aux_char = bs.get_cstr();
    std::string endpoint( (endpoint_char) ? endpoint_char : std::string() );
    std::string aux( (aux_char) ? aux_char : std::string() );

    uint8_t srv_type = bs.get_u8();
    bool is_learner = (srv_type & LEARNER_FLAG) ? true : false;
    bool new_joiner = (srv_type & NEW_JOINER_FLAG) ? true : false;

    int32 priority = bs.get_i32();

    ptr<srv_config> ret =
        cs_new<srv_config>(id, dc_id, endpoint, aux, is_learner, priority);
    ret->set_new_joiner(new_joiner);
    return ret;
}

ptr<buffer> srv_config::serialize() const{
    ptr<buffer> buf = buffer::alloc(
        sz_int +
        sz_int +
        (endpoint_.length()+1) +
        (aux_.length()+1) +
        1 +
        sz_int
    );
    buf->put(id_);
    buf->put(dc_id_);
    buf->put(endpoint_);
    buf->put(aux_);

    uint8_t srv_type = 0x0;
    srv_type |= (learner_ ? LEARNER_FLAG : 0x0);
    srv_type |= (new_joiner_ ? NEW_JOINER_FLAG : 0x0);
    buf->put((byte)srv_type);

    buf->put(priority_);
    buf->pos(0);
    return buf;
}

} // namespace nuraft;
