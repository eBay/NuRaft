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

#ifndef _LOG_VALUE_TYPE_HXX_
#define _LOG_VALUE_TYPE_HXX_

namespace nuraft {

enum log_val_type {
    app_log         = 1,
    conf            = 2,
    cluster_server  = 3,
    log_pack        = 4,
    snp_sync_req    = 5,
    custom          = 999,
};

}
#endif // _LOG_VALUE_TYPE_HXX_
