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

#include "snapshot_sync_req.hxx"

#include <cstring>

namespace nuraft {

std::shared_ptr< snapshot_sync_req > snapshot_sync_req::deserialize(buffer& buf) {
    buffer_serializer bs(buf);
    return deserialize(bs);
}

std::shared_ptr< snapshot_sync_req > snapshot_sync_req::deserialize(buffer_serializer& bs) {
    std::shared_ptr< snapshot > snp(snapshot::deserialize(bs));
    uint64_t offset = bs.get_u64();
    bool done = bs.get_u8() == 1;
    auto src = reinterpret_cast< std::byte const* >(bs.data());
    std::shared_ptr< buffer > b;
    if (bs.pos() < bs.size()) {
        size_t sz = bs.size() - bs.pos();
        b = buffer::alloc(sz);
        ::memcpy(b->data(), src, sz);
    } else {
        b = buffer::alloc(0);
    }

    return std::make_shared< snapshot_sync_req >(snp, offset, b, done);
}

std::shared_ptr< buffer > snapshot_sync_req::serialize() {
    std::shared_ptr< buffer > snp_buf = snapshot_->serialize();
    std::shared_ptr< buffer > buf =
        buffer::alloc(snp_buf->size() + sz_uint64_t + sz_byte + (data_->size() - data_->pos()));
    buf->put(*snp_buf);
    buf->put(offset_);
    buf->put(done_ ? std::byte{0x01} : std::byte{0x00});
    buf->put(*data_);
    buf->pos(0);
    return buf;
}

} // namespace nuraft
