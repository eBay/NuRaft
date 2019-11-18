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

#pragma once

#include "pp_util.hxx"
#include "ptr.hxx"

#include <cstdint>

namespace nuraft {

class buffer;
class buffer_serializer {
public:
    enum endianness {
        LITTLE = 0x0,
        BIG = 0x1,
    };

    buffer_serializer(buffer& src_buf,
                      endianness endian = LITTLE);

    buffer_serializer(ptr<buffer>& src_buf_ptr,
                      endianness endian = LITTLE);

    __nocopy__(buffer_serializer);

public:
    /**
     * Get the current position of cursor inside buffer.
     *
     * @return Position of cursor.
     */
    inline size_t pos() const { return pos_; }

    /**
     * Get the size of given buffer.
     *
     * @return Size of buffer.
     */
    size_t size() const;

    /**
     * Set the position of cursor.
     *
     * @param p New position.
     */
    void pos(size_t new_pos);

    /**
     * Get the memory pointer to the current position.
     *
     * @return Pointer to the current position.
     */
    void* data() const;

    /**
     * Put 1-byte unsigned integer.
     *
     * @param val 1-byte unsigned integer.
     */
    void put_u8(uint8_t val);

    /**
     * Put 2-byte unsigned integer.
     *
     * @param val 2-byte unsigned integer.
     */
    void put_u16(uint16_t val);

    /**
     * Put 4-byte unsigned integer.
     *
     * @param val 4-byte unsigned integer.
     */
    void put_u32(uint32_t val);

    /**
     * Put 8-byte unsigned integer.
     *
     * @param val 8-byte unsigned integer.
     */
    void put_u64(uint64_t val);

    /**
     * Put 1-byte signed integer.
     *
     * @param val 1-byte signed integer.
     */
    void put_i8(int8_t val);

    /**
     * Put 2-byte signed integer.
     *
     * @param val 2-byte signed integer.
     */
    void put_i16(int16_t val);

    /**
     * Put 4-byte signed integer.
     *
     * @param val 4-byte signed integer.
     */
    void put_i32(int32_t val);

    /**
     * Put 8-byte signed integer.
     *
     * @param val 8-byte signed integer.
     */
    void put_i64(int64_t val);

    /**
     * Put a byte array.
     *
     * WARNING:
     *   It does not put the given length info,
     *   so caller should know the length of byte array in advance
     *   before calling `get_raw(size_t)`.
     *
     *   If not, please use `put_bytes(const void* raw_ptr, size_t len);`.
     *
     * @param raw_ptr Pointer to the byte array.
     * @param len Length of the byte array.
     */
    void put_raw(const void* raw_ptr, size_t len);

    /**
     * Put given buffer.
     * If given buffer's position is not 0, only data
     * after that position will be copied.
     *
     * WARNING:
     *   It does not put length info of given buffer,
     *   so caller should know the length of buffer in advance
     *   before calling `get_buffer(ptr<buffer>)`.
     *
     *   If not, please use `put_bytes(const void* raw_ptr, size_t len);`.
     *
     * @param buf Buffer.
     */
    void put_buffer(const buffer& buf);

    /**
     * Put a byte array.
     * This function will put 4-byte length first, and then
     * the actual byte array next.
     *
     * @param raw_ptr Pointer to the byte array.
     * @param len Length of the byte array.
     */
    void put_bytes(const void* raw_ptr, size_t len);

    /**
     * Put a string.
     * This function will put 4-byte length first, and then
     * the actual string next.
     *
     * @param str String.
     */
    void put_str(const std::string& str);

    /**
     * Put a C-style string, which ends with NULL character.
     *
     * If you want to put generic binary,
     * please use `put(const byte*, size_t)`.
     *
     * @param str String.
     */
    void put_cstr(const char* str);

    /**
     * Get 1-byte unsigned integer.
     *
     * @return 1-byte unsinged integer.
     */
    uint8_t get_u8();

    /**
     * Get 2-byte unsigned integer.
     *
     * @return 2-byte unsinged integer.
     */
    uint16_t get_u16();

    /**
     * Get 4-byte unsigned integer.
     *
     * @return 4-byte unsinged integer.
     */
    uint32_t get_u32();

    /**
     * Get 8-byte unsigned integer.
     *
     * @return 8-byte unsinged integer.
     */
    uint64_t get_u64();

    /**
     * Get 1-byte signed integer.
     *
     * @return 1-byte singed integer.
     */
    int8_t get_i8();

    /**
     * Get 2-byte signed integer.
     *
     * @return 2-byte singed integer.
     */
    int16_t get_i16();

    /**
     * Get 4-byte signed integer.
     *
     * @return 4-byte singed integer.
     */
    int32_t get_i32();

    /**
     * Get 8-byte signed integer.
     *
     * @return 8-byte singed integer.
     */
    int64_t get_i64();

    /**
     * Read byte array of given size.
     * It will NOT allocate a new memory, but return the
     * reference to the memory inside buffer only.
     *
     * @param len Size to read.
     * @return Pointer to the starting point of byte array.
     */
    void* get_raw(size_t len);

    /**
     * Read byte array of given buffer's size,
     * and copy it to the given buffer.
     * If buffer's position is not zero, data will be copied
     * starting from that position.
     *
     * @param dst Buffer where the data will be stored.
     */
    void get_buffer(ptr<buffer>& dst);

    /**
     * Read 4-byte length followed by byte array, and then return them.
     * It will NOT allocate a new memory, but return the
     * reference to the memory inside buffer only.
     *
     * @param[out] len Size of returned byte array.
     * @return Pointer to the starting point of byte array.
     */
    void* get_bytes(size_t& len);

    /**
     * Read 4-byte length followed by string, and then return it
     * as a string instance.
     *
     * @return String instance.
     */
    std::string get_str();

    /**
     * Read C-style string (null terminated).
     * It will NOT allocate a new memory, but return the
     * reference to the memory inside buffer only.
     *
     * @return C-style string.
     */
    const char* get_cstr();

private:
    bool is_valid(size_t len) const;

    // Endianness.
    endianness endian_;

    // Reference to buffer to read or write.
    buffer& buf_;

    // Current position.
    size_t pos_;
};

}

