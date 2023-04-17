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

#include "buffer.hxx"
#include "stat_mgr.hxx"

#include <cstddef>
#include <cstring>
#include <iostream>

#define __is_big_block(p) (0x80000000 & *((uint32_t*)(p)))

#define __init_block(ptr, len, type)                                                                                   \
    ((type*)(ptr))[0] = (type)len;                                                                                     \
    ((type*)(ptr))[1] = 0

#define __init_s_block(p, l) __init_block(p, l, uint16_t)

#define __init_b_block(p, l)                                                                                           \
    __init_block(p, l, uint32_t);                                                                                      \
    *((uint32_t*)(p)) |= 0x80000000

#define __pos_of_s_block(p) ((uint16_t*)(p))[1]

#define __pos_of_b_block(p) ((uint32_t*)(p))[1]

#define __size_of_block(p) (__is_big_block(p)) ? (*((uint32_t*)(p)) ^ 0x80000000) : *((uint16_t*)(p))

#define __pos_of_block(p) (__is_big_block(p)) ? __pos_of_b_block(p) : __pos_of_s_block(p)

#define __mv_fw_block(ptr, delta)                                                                                      \
    if (__is_big_block(ptr)) {                                                                                         \
        ((uint32_t*)(ptr))[1] += (delta);                                                                              \
    } else {                                                                                                           \
        ((uint16_t*)(ptr))[1] += (uint16_t)(delta);                                                                    \
    }

#define __set_block_pos(ptr, pos)                                                                                      \
    if (__is_big_block(ptr)) {                                                                                         \
        ((uint32_t*)(ptr))[1] = (pos);                                                                                 \
    } else {                                                                                                           \
        ((uint16_t*)(ptr))[1] = (uint16_t)(pos);                                                                       \
    }

#define __data_of_block(p)                                                                                             \
    (__is_big_block(p)) ? (byte*)(((byte*)(((uint32_t*)(p)) + 2)) + __pos_of_b_block(p))                               \
                        : (byte*)(((byte*)(((uint16_t*)(p)) + 2)) + __pos_of_s_block(p))

#define __entire_data_of_block(p)                                                                                      \
    (__is_big_block(p)) ? (byte*)((byte*)(((uint32_t*)(p)) + 2)) : (byte*)((byte*)(((uint16_t*)(p)) + 2))

namespace nuraft {

using std::byte;

static void free_buffer(buffer* buf) {
    static stat_elem& num_active = *stat_mgr::get_instance()->create_stat(stat_elem::COUNTER, "num_active_buffers");
    static stat_elem& amount_active =
        *stat_mgr::get_instance()->create_stat(stat_elem::COUNTER, "amount_active_buffers");

    num_active--;
    amount_active -= buf->container_size();

    delete[] reinterpret_cast< char* >(buf);
}

std::shared_ptr< buffer > buffer::alloc(const size_t size) {
    static stat_elem& num_allocs = *stat_mgr::get_instance()->create_stat(stat_elem::COUNTER, "num_buffer_allocs");
    static stat_elem& amount_allocs =
        *stat_mgr::get_instance()->create_stat(stat_elem::COUNTER, "amount_buffer_allocs");
    static stat_elem& num_active = *stat_mgr::get_instance()->create_stat(stat_elem::COUNTER, "num_active_buffers");
    static stat_elem& amount_active =
        *stat_mgr::get_instance()->create_stat(stat_elem::COUNTER, "amount_active_buffers");

    if (size >= 0x80000000) {
        throw std::out_of_range("size exceed the max size that "
                                "nuraft::buffer could support");
    }
    num_allocs++;
    num_active++;

    if (size >= 0x8000) {
        size_t len = size + sizeof(uint32_t) * 2;
        std::shared_ptr< buffer > buf(reinterpret_cast< buffer* >(new char[len]), &free_buffer);
        amount_allocs += len;
        amount_active += len;

        auto ptr = reinterpret_cast< uint32_t* >(buf.get());
        __init_b_block(ptr, size);
        return buf;
    }

    size_t len = size + sizeof(uint16_t) * 2;
    std::shared_ptr< buffer > buf(reinterpret_cast< buffer* >(new char[len]), &free_buffer);
    amount_allocs += len;
    amount_active += len;

    auto ptr = reinterpret_cast< uint16_t* >(buf.get());
    __init_s_block(ptr, size);

    return buf;
}

std::shared_ptr< buffer > buffer::copy(const buffer& buf) {
    std::shared_ptr< buffer > other = alloc(buf.size() - buf.pos());
    other->put(buf);
    other->pos(0);
    return other;
}

std::shared_ptr< buffer > buffer::clone(const buffer& buf) {
    std::shared_ptr< buffer > other = alloc(buf.size());

    byte* dst = other->data_begin();
    byte* src = buf.data_begin();
    ::memcpy(dst, src, buf.size());

    other->pos(0);
    return other;
}

std::shared_ptr< buffer > buffer::expand(const buffer& buf, uint32_t new_size) {
    if (new_size >= 0x80000000) {
        throw std::out_of_range("size exceed the max size that "
                                "nuraft::buffer could support");
    }

    if (new_size < buf.size()) {
        throw std::out_of_range("realloc() new_size is less than "
                                "old size");
    }
    std::shared_ptr< buffer > other = alloc(new_size);
    byte* dst = other->data_begin();
    byte* src = buf.data_begin();
    ::memcpy(dst, src, buf.size());
    other->pos(buf.pos());
    return other;
}

size_t buffer::container_size() const {
    return (size_t)(__size_of_block(this) + ((__is_big_block(this)) ? sizeof(uint32_t) * 2 : sizeof(uint16_t) * 2));
}

size_t buffer::size() const { return (size_t)(__size_of_block(this)); }

size_t buffer::pos() const { return (size_t)(__pos_of_block(this)); }

byte* buffer::data() const { return __data_of_block(this); }

byte* buffer::data_begin() const { return __entire_data_of_block(this); }

int32_t buffer::get_int() {
    size_t avail = size() - pos();
    if (avail < sz_int) { throw std::overflow_error("insufficient buffer available for an int32_t value"); }

    byte* d = data();
    int32_t val = 0;
    for (size_t i = 0; i < sz_int; ++i) {
        int32_t byte_val = (int32_t) * (d + i);
        val += (byte_val << (i * 8));
    }

    __mv_fw_block(this, sz_int);
    return val;
}

uint64_t buffer::get_uint64() {
    size_t avail = size() - pos();
    if (avail < sz_uint64_t) { throw std::overflow_error("insufficient buffer available for an uint64_t value"); }

    byte* d = data();
    uint64_t val = 0L;
    for (size_t i = 0; i < sz_uint64_t; ++i) {
        uint64_t byte_val = (uint64_t) * (d + i);
        val += (byte_val << (i * 8));
    }

    __mv_fw_block(this, sz_uint64_t);
    return val;
}

byte buffer::get_byte() {
    size_t avail = size() - pos();
    if (avail < sz_byte) { throw std::overflow_error("insufficient buffer available for a byte"); }

    byte val = *data();
    __mv_fw_block(this, sz_byte);
    return val;
}

const byte* buffer::get_bytes(size_t& len) {
    size_t avail = size() - pos();
    if (avail < sz_int) { throw std::overflow_error("insufficient buffer available for a bytes length (int32_t)"); }

    byte* d = data();
    int32_t val = 0;
    for (size_t i = 0; i < sz_int; ++i) {
        int32_t byte_val = (int32_t) * (d + i);
        val += (byte_val << (i * 8));
    }

    __mv_fw_block(this, sz_int);
    len = val;

    d = data();
    if (size() - pos() < len) { throw std::overflow_error("insufficient buffer available for a byte array"); }

    __mv_fw_block(this, len);
    return reinterpret_cast< const byte* >(d);
}

void buffer::pos(size_t p) {
    size_t position = (p > size()) ? size() : p;
    __set_block_pos(this, position);
}

const char* buffer::get_str() {
    size_t p = pos();
    size_t s = size();
    size_t i = 0;
    byte* d = data();
    while ((p + i) < s && (*(d + i) != byte{0x0}))
        ++i;
    if (i == 0) {
        // Empty string, move forward 1 byte for NULL character.
        __mv_fw_block(this, i + 1);
    }
    if (p + i >= s || i == 0) { return nilptr; }

    __mv_fw_block(this, i + 1);
    return reinterpret_cast< const char* >(d);
}

void buffer::put(byte b) {
    if (size() - pos() < sz_byte) { throw std::overflow_error("insufficient buffer to store byte"); }

    byte* d = data();
    *d = b;
    __mv_fw_block(this, sz_byte);
}

void buffer::put(const char* ba, size_t len) { put((const byte*)ba, len); }

void buffer::put(const byte* ba, size_t len) {
    // put length as int32_t first
    if (size() - pos() < sz_int) { throw std::overflow_error("insufficient buffer to store int32_t length"); }

    byte* d = data();
    for (size_t i = 0; i < sz_int; ++i) {
        *(d + i) = (byte)(len >> (i * 8));
    }

    __mv_fw_block(this, sz_int);

    // put bytes
    put_raw(ba, len);
}

void buffer::put(int32_t val) {
    if (size() - pos() < sz_int) { throw std::overflow_error("insufficient buffer to store int32_t"); }

    byte* d = data();
    for (size_t i = 0; i < sz_int; ++i) {
        *(d + i) = (byte)(val >> (i * 8));
    }

    __mv_fw_block(this, sz_int);
}

void buffer::put(uint64_t val) {
    if (size() - pos() < sz_uint64_t) { throw std::overflow_error("insufficient buffer to store unsigned long"); }

    byte* d = data();
    for (size_t i = 0; i < sz_uint64_t; ++i) {
        *(d + i) = (byte)(val >> (i * 8));
    }

    __mv_fw_block(this, sz_uint64_t);
}

void buffer::put(const std::string& str) {
    if (size() - pos() < (str.length() + 1)) { throw std::overflow_error("insufficient buffer to store a string"); }

    byte* d = data();
    for (size_t i = 0; i < str.length(); ++i) {
        *(d + i) = (byte)str[i];
    }

    *(d + str.length()) = (byte)0;
    __mv_fw_block(this, str.length() + 1);
}

void buffer::get(std::shared_ptr< buffer >& dst) {
    size_t sz = dst->size() - dst->pos();
    ::memcpy(dst->data(), data(), sz);
    __mv_fw_block(this, sz);
}

void buffer::put(const buffer& buf) {
    size_t sz = size();
    size_t p = pos();
    size_t src_sz = buf.size();
    size_t src_p = buf.pos();
    if ((sz - p) < (src_sz - src_p)) { throw std::overflow_error("insufficient buffer to hold the other buffer"); }

    byte* d = data();
    byte* src = buf.data();
    ::memcpy(d, src, src_sz - src_p);
    __mv_fw_block(this, src_sz - src_p);
}

byte* buffer::get_raw(size_t len) {
    if (size() - pos() < len) { throw std::overflow_error("insufficient buffer available for a raw byte array"); }

    byte* d = data();
    __mv_fw_block(this, len);
    return d;
}

void buffer::put_raw(const byte* ba, size_t len) {
    if (size() - pos() < len) { throw std::overflow_error("insufficient buffer to store a raw byte array"); }

    ::memcpy(data(), ba, len);
    __mv_fw_block(this, len);
}

} // namespace nuraft
using namespace nuraft;

std::ostream& nuraft::operator<<(std::ostream& out, buffer& buf) {
    if (!out) { throw std::ios::failure("bad output stream."); }

    out.write(reinterpret_cast< char* >(buf.data()), buf.size() - buf.pos());

    if (!out) { throw std::ios::failure("write failed"); }

    return out;
}

std::istream& nuraft::operator>>(std::istream& in, buffer& buf) {
    if (!in) { throw std::ios::failure("bad input stream"); }

    char* data = reinterpret_cast< char* >(buf.data());
    int size = buf.size() - buf.pos();
    in.read(data, size);

    if (!in) { throw std::ios::failure("read failed"); }

    return in;
}
