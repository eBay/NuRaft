/************************************************************************
Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

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

#include "buffer.hxx"
#include "buffer_serializer.hxx"

#include <cstring>
#include <stdexcept>

#define put16l(val, ptr)                                                                                               \
    {                                                                                                                  \
        ptr[0] = static_cast< byte >(val & 0xff);                                                                      \
        ptr[1] = static_cast< byte >(val >> 8);                                                                        \
    }

#define put16b(val, ptr)                                                                                               \
    {                                                                                                                  \
        ptr[1] = static_cast< byte >(val & 0xff);                                                                      \
        ptr[0] = static_cast< byte >(val >> 8);                                                                        \
    }

#define put32l(val, ptr)                                                                                               \
    {                                                                                                                  \
        put16l(static_cast< uint16_t >(val & 0xffff), ptr);                                                            \
        put16l(static_cast< uint16_t >(val >> 16), (ptr + sizeof(uint16_t)));                                          \
    }

#define put32b(val, ptr)                                                                                               \
    {                                                                                                                  \
        put16b(static_cast< uint16_t >(val & 0xffff), (ptr + sizeof(uint16_t)));                                       \
        put16b(static_cast< uint16_t >(val >> 16), ptr);                                                               \
    }

#define put64l(val, ptr)                                                                                               \
    {                                                                                                                  \
        put32l(static_cast< uint32_t >(val & 0xffffffff), ptr);                                                        \
        put32l(static_cast< uint32_t >(val >> 32), (ptr + sizeof(uint32_t)));                                          \
    }

#define put64b(val, ptr)                                                                                               \
    {                                                                                                                  \
        put32b(static_cast< uint32_t >(val & 0xffffffff), (ptr + sizeof(uint32_t)));                                   \
        put32b(static_cast< uint32_t >(val >> 32), ptr);                                                               \
    }

#define get16l(ptr, val)                                                                                               \
    {                                                                                                                  \
        val |= std::to_integer< uint8_t >(ptr[1]);                                                                     \
        val <<= 8;                                                                                                     \
        val |= std::to_integer< uint8_t >(ptr[0]);                                                                     \
    }

#define get16b(ptr, val)                                                                                               \
    {                                                                                                                  \
        val |= std::to_integer< uint16_t >(ptr[0]);                                                                    \
        val <<= 8;                                                                                                     \
        val |= std::to_integer< uint16_t >(ptr[1]);                                                                    \
    }

#define get32l(ptr, val)                                                                                               \
    {                                                                                                                  \
        get16l((ptr + sizeof(uint16_t)), val);                                                                         \
        val <<= 8;                                                                                                     \
        get16l((ptr), val);                                                                                            \
    }

#define get32b(ptr, val)                                                                                               \
    {                                                                                                                  \
        get16b((ptr), val);                                                                                            \
        val <<= 8;                                                                                                     \
        get16b((ptr + sizeof(uint16_t)), val);                                                                         \
    }

#define get64l(ptr, val)                                                                                               \
    {                                                                                                                  \
        get32l((ptr + sizeof(uint32_t)), val);                                                                         \
        val <<= 8;                                                                                                     \
        get32l((ptr), val);                                                                                            \
    }

#define get64b(ptr, val)                                                                                               \
    {                                                                                                                  \
        get32b((ptr), val);                                                                                            \
        val <<= 8;                                                                                                     \
        get32b((ptr + sizeof(uint32_t)), val);                                                                         \
    }

#define chk_length(val)                                                                                                \
    if (!is_valid(sizeof(val))) throw std::overflow_error("not enough space")

namespace nuraft {

buffer_serializer::buffer_serializer(buffer& src_buf, buffer_serializer::endianness endian) :
        endian_(endian), buf_(src_buf), pos_(0) {}

buffer_serializer::buffer_serializer(std::shared_ptr< buffer >& src_buf_ptr, buffer_serializer::endianness endian) :
        endian_(endian), buf_(*src_buf_ptr), pos_(0) {}

size_t buffer_serializer::size() const { return buf_.size(); }

void buffer_serializer::pos(size_t new_pos) {
    if (new_pos > buf_.size()) throw std::overflow_error("invalid position");
    pos_ = new_pos;
}

void* buffer_serializer::data() const {
    uint8_t* ptr = (uint8_t*)buf_.data_begin();
    return ptr + pos();
}

bool buffer_serializer::is_valid(size_t len) const {
    if (pos() + len > buf_.size()) return false;
    return true;
}

void buffer_serializer::put_u8(uint8_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    ptr[0] = std::byte{val};
    pos(pos() + sizeof(std::byte));
}

void buffer_serializer::put_u8(std::byte val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    ptr[0] = val;
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_u16(uint16_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        put16l(val, ptr);
    } else {
        put16b(val, ptr);
    }
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_u32(uint32_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        put32l(val, ptr);
    } else {
        put32b(val, ptr);
    }
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_u64(uint64_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        put64l(val, ptr);
    } else {
        put64b(val, ptr);
    }
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_i8(int8_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    ptr[0] = static_cast< byte >(val);
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_i16(int16_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        put16l(val, ptr);
    } else {
        put16b(val, ptr);
    }
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_i32(int32_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        put32l(val, ptr);
    } else {
        put32b(val, ptr);
    }
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_i64(int64_t val) {
    chk_length(val);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        put64l(val, ptr);
    } else {
        put64b(val, ptr);
    }
    pos(pos() + sizeof(val));
}

void buffer_serializer::put_raw(const void* raw_ptr, size_t len) {
    if (!is_valid(len)) throw std::overflow_error("not enough space");
    memcpy(data(), raw_ptr, len);
    pos(pos() + len);
}

void buffer_serializer::put_buffer(const buffer& buf) {
    size_t len = buf.size() - buf.pos();
    put_raw(buf.data(), len);
}

void buffer_serializer::put_bytes(const void* raw_ptr, size_t len) {
    if (!is_valid(len + sizeof(uint32_t))) { throw std::overflow_error("not enough space"); }
    put_u32(len);
    put_raw(raw_ptr, len);
}

void buffer_serializer::put_str(const std::string& str) { put_bytes(str.data(), str.size()); }

void buffer_serializer::put_cstr(const char* str) {
    size_t local_pos = pos_;
    size_t buf_size = buf_.size();
    char* ptr = (char*)buf_.data_begin();

    size_t ii = 0;
    while (str[ii] != 0x0) {
        if (local_pos >= buf_size) { throw std::overflow_error("not enough space"); }
        ptr[local_pos] = str[ii];
        local_pos++;
        ii++;
    }
    // Put NULL character at the end.
    if (local_pos >= buf_size) { throw std::overflow_error("not enough space"); }
    ptr[local_pos++] = 0x0;
    pos(local_pos);
}

uint8_t buffer_serializer::get_u8() {
    uint8_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    ret = std::to_integer< uint8_t >(ptr[0]);
    pos(pos() + sizeof(ret));
    return ret;
}

uint16_t buffer_serializer::get_u16() {
    uint16_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        get16l(ptr, ret);
    } else {
        get16b(ptr, ret);
    }
    pos(pos() + sizeof(ret));
    return ret;
}

uint32_t buffer_serializer::get_u32() {
    uint32_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        get32l(ptr, ret);
    } else {
        get32b(ptr, ret);
    }
    pos(pos() + sizeof(ret));
    return ret;
}

uint64_t buffer_serializer::get_u64() {
    uint64_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        get64l(ptr, ret);
    } else {
        get64b(ptr, ret);
    }
    pos(pos() + sizeof(ret));
    return ret;
}

int8_t buffer_serializer::get_i8() {
    int8_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    ret = std::to_integer< int8_t >(ptr[0]);
    pos(pos() + sizeof(ret));
    return ret;
}

int16_t buffer_serializer::get_i16() {
    int16_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        get16l(ptr, ret);
    } else {
        get16b(ptr, ret);
    }
    pos(pos() + sizeof(ret));
    return ret;
}

int32_t buffer_serializer::get_i32() {
    int32_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        get32l(ptr, ret);
    } else {
        get32b(ptr, ret);
    }
    pos(pos() + sizeof(ret));
    return ret;
}

int64_t buffer_serializer::get_i64() {
    int64_t ret = 0;
    chk_length(ret);
    auto ptr = buf_.data_begin() + pos_;
    if (endian_ == LITTLE) {
        get64l(ptr, ret);
    } else {
        get64b(ptr, ret);
    }
    pos(pos() + sizeof(ret));
    return ret;
}

void* buffer_serializer::get_raw(size_t len) {
    auto ptr = buf_.data_begin() + pos_;
    pos(pos() + len);
    return ptr;
}

void buffer_serializer::get_buffer(std::shared_ptr< buffer >& dst) {
    size_t len = dst->size() - dst->pos();
    void* ptr = get_raw(len);
    ::memcpy(dst->data(), ptr, len);
}

void* buffer_serializer::get_bytes(size_t& len) {
    len = get_u32();
    if (!is_valid(len)) throw std::overflow_error("not enough space");
    return get_raw(len);
}

std::string buffer_serializer::get_str() {
    size_t len = 0;
    void* data = get_bytes(len);
    if (!data) return std::string();
    return std::string((const char*)data, len);
}

const char* buffer_serializer::get_cstr() {
    char* ptr = (char*)buf_.data_begin() + pos_;
    size_t len = strlen(ptr);
    pos(pos() + len + 1);
    return ptr;
}

} // namespace nuraft
