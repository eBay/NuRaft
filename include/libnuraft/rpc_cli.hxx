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

#ifndef _RPC_CLI_HXX_
#define _RPC_CLI_HXX_

#include "async.hxx"
#include "req_msg.hxx"
#include "resp_msg.hxx"
#include "rpc_exception.hxx"

#include <cstdint>

namespace nuraft {

class resp_msg;

using rpc_result = async_result<ptr<resp_msg>, ptr<rpc_exception>>;

using rpc_handler = rpc_result::handler_type;

class rpc_client {
    __interface_body__(rpc_client);

public:
    virtual void send(ptr<req_msg>& req, rpc_handler& when_done) = 0;

    virtual uint64_t get_id() const = 0;
};

}

#endif //_RPC_CLI_HXX_
