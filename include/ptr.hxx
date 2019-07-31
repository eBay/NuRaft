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

#ifndef _CS_PTR_HXX_
#define _CS_PTR_HXX_

#include <memory>

namespace nuraft {

template<typename T>
using ptr = std::shared_ptr<T>;

template<typename T>
using wptr = std::weak_ptr<T>;

template<typename T, typename ... TArgs>
inline ptr<T> cs_new(TArgs&&... args) {
    return std::make_shared<T>(std::forward<TArgs>(args)...);
}

}
#endif //_CS_PTR_HXX_
