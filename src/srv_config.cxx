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

ptr<srv_config> srv_config::deserialize(buffer& buf) {
    int32 id = buf.get_int();
    int32 dc_id = buf.get_int();
    const char* endpoint_char = buf.get_str();
    const char* aux_char = buf.get_str();
    std::string endpoint( (endpoint_char) ? endpoint_char : std::string() );
    std::string aux( (aux_char) ? aux_char : std::string() );
    byte is_learner = buf.get_byte();
    int32 priority = buf.get_int();
    return cs_new<srv_config>(id, dc_id, endpoint, aux, is_learner, priority);
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
    buf->put((byte)(learner_?(1):(0)));
    buf->put(priority_);
    buf->pos(0);
    return buf;
}

} // namespace nuraft;
