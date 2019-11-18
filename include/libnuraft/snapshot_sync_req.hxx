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

#ifndef _SNAPSHOT_SYNC_REQ_HXX_
#define _SNAPSHOT_SYNC_REQ_HXX_

#include "buffer.hxx"
#include "buffer_serializer.hxx"
#include "pp_util.hxx"
#include "ptr.hxx"
#include "snapshot.hxx"

namespace nuraft {

class snapshot;
class snapshot_sync_req {
public:
    snapshot_sync_req(const ptr<snapshot>& s,
                      ulong offset,
                      const ptr<buffer>& buf,
                      bool done)
        : snapshot_(s), offset_(offset), data_(buf), done_(done) {}

    __nocopy__(snapshot_sync_req);

public:
    static ptr<snapshot_sync_req> deserialize(buffer& buf);

    static ptr<snapshot_sync_req> deserialize(buffer_serializer& bs);

    snapshot& get_snapshot() const {
        return *snapshot_;
    }

    ulong get_offset() const { return offset_; }
    void set_offset(const ulong src) { offset_ = src; }

    buffer& get_data() const { return *data_; }

    bool is_done() const { return done_; }

    ptr<buffer> serialize();
private:
    ptr<snapshot> snapshot_;
    ulong offset_;
    ptr<buffer> data_;
    bool done_;
};

}

#endif //_SNAPSHOT_SYNC_REQ_HXX_
