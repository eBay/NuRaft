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

#include "snapshot.hxx"

#include "cluster_config.hxx"

namespace nuraft {

ptr<snapshot> snapshot::deserialize(buffer& buf) {
    buffer_serializer bs(buf);
    return deserialize(bs);
}

ptr<snapshot> snapshot::deserialize(buffer_serializer& bs) {
    type snp_type = static_cast<type>(bs.get_u8());
    ulong last_log_idx = bs.get_u64();
    ulong last_log_term = bs.get_u64();
    ulong size = bs.get_u64();
    ptr<cluster_config> conf( cluster_config::deserialize(bs) );
    return cs_new<snapshot>(last_log_idx, last_log_term, conf, size, snp_type);
}

ptr<buffer> snapshot::serialize() {
    ptr<buffer> conf_buf = last_config_->serialize();
    ptr<buffer> buf = buffer::alloc(conf_buf->size() + sz_ulong * 3 + sz_byte);
    buf->put((byte)type_);
    buf->put(last_log_idx_);
    buf->put(last_log_term_);
    buf->put(size_);
    buf->put(*conf_buf);
    buf->pos(0);
    return buf;
}

} // namespace nuraft;

