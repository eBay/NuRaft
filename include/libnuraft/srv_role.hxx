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

#ifndef _SRV_ROLE_HXX_
#define _SRV_ROLE_HXX_

namespace nuraft {

#include "attr_unused.hxx"

enum srv_role {
    follower    = 0x1,
    candidate   = 0x2,
    leader      = 0x3
};

static std::string ATTR_UNUSED
       srv_role_to_string(srv_role _role)
{
    switch (_role) {
    case follower:      return "follower";
    case candidate:     return "candidate";
    case leader:        return "leader";
    default:            return "UNKNOWN";
    }
    return "UNKNOWN";
}

}

#endif
