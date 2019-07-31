/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/stbrumme/crc32

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

#ifndef _JSAHN_CRC32_H
#define _JSAHN_CRC32_H

#include <stdint.h>
#if defined(__linux__) || defined(__APPLE__)
    #include <unistd.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

uint32_t crc32_1(const void* data, size_t len, uint32_t prev_value);
uint32_t crc32_8(const void* data, size_t len, uint32_t prev_value);
uint32_t crc32_8_last8(const void* data, size_t len, uint32_t prev_value);

#ifdef __cplusplus
}
#endif

#endif
