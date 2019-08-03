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

#ifndef _RPC_EXCEPTION_HXX_
#define _RPC_EXCEPTION_HXX_

#include <exception>
#include <string>

namespace nuraft {

class req_msg;
class rpc_exception : public std::exception {
public:
    rpc_exception(const std::string& err, ptr<req_msg> req)
        : req_(req), err_(err.c_str()) {}

    __nocopy__(rpc_exception);
public:
    ptr<req_msg> req() const { return req_; }

    virtual const char* what() const throw() __override__ {
        return err_.c_str();
    }
private:
    ptr<req_msg> req_;
    std::string err_;
};

}

#endif //_RPC_EXCEPTION_HXX_
