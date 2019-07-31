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

#include "nuraft.hxx"

#include "test_common.h"

#include <cstring>
#include <random>
#include <string>

using namespace nuraft;

namespace buffer_test {

int buffer_basic_test(size_t buf_size) {
    ptr<buffer> buf = buffer::alloc(buf_size);

    uint seed = (uint)std::chrono::system_clock::now()
                .time_since_epoch().count();
    std::default_random_engine engine(seed);
    std::uniform_int_distribution<int32> distribution(1, 10000);
    auto rnd = std::bind(distribution, engine);

    // store int32 values into buffer
    std::vector<int32> vals;
    for (int i = 0; i < 100; ++i) {
        int32 val = rnd();
        vals.push_back(val);
        buf->put(val);
    }

    CHK_EQ( 100 * sz_int, buf->pos() );

    ulong long_val = std::numeric_limits<uint>::max();
    long_val += rnd();
    buf->put(long_val);

    byte b = (byte)rnd();
    buf->put(b);
    buf->put("a string");

    byte b1 = (byte)rnd();
    buf->put(b1);

    const char raw_str[] = "a raw string";
    buf->put_raw(reinterpret_cast<const byte*>(raw_str), sizeof(raw_str));

    ptr<buffer> buf1(buffer::alloc(100));
    buf1->put("another string");
    buf1->pos(0);

    ptr<buffer> buf2(buffer::copy(*buf1));
    buf->put(*buf1);
    buf->pos(0);

    ptr<buffer> buf3(buffer::alloc(sz_int * 100));
    buf->get(buf3);
    buf->pos(0);

    for (int i = 0; i < 100; ++i) {
        int32 val = buf->get_int();
        CHK_EQ( vals[i], val );
    }

    buf3->pos(0);
    for (int i = 0; i < 100; ++i) {
        int32 val = buf3->get_int();
        CHK_EQ( vals[i], val );
    }

    CHK_EQ( long_val, buf->get_ulong() );
    CHK_EQ( b, buf->get_byte() );
    CHK_EQ( std::string("a string"),
            std::string(buf->get_str()) );
    CHK_EQ( b1, buf->get_byte() );
    CHK_EQ( 0, std::memcmp(raw_str, buf->get_raw(sizeof(raw_str)), sizeof(raw_str)) );
    CHK_EQ( std::string("another string"),
            std::string(buf->get_str()) );
    CHK_EQ( std::string("another string"),
            std::string(buf2->get_str()) );
    CHK_EQ( ( 100 * sz_int +
              2 * sz_byte + sizeof(raw_str) +
              sz_ulong + strlen("a string") + 1 + strlen("another string") + 1 ),
            buf->pos() );

    std::stringstream stream;
    long_val = std::numeric_limits<uint>::max();
    long_val += rnd();
    ptr<buffer> lbuf(buffer::alloc(sizeof(ulong)));
    lbuf->put(long_val);
    lbuf->pos(0);

    stream << *lbuf;
    stream.seekp(0);

    ptr<buffer> lbuf1(buffer::alloc(sizeof(ulong)));
    stream >> *lbuf1;

    ulong long_val_copy = lbuf1->get_ulong();
    CHK_EQ( long_val, long_val_copy );

    return 0;
}

int buffer_serializer_test(bool little_endian) {
    ptr<buffer> buf = buffer::alloc(100);
    buffer_serializer::endianness endian = (little_endian)
                                           ? buffer_serializer::LITTLE
                                           : buffer_serializer::BIG;
    buffer_serializer ss(buf, endian);

    CHK_Z( ss.pos() );

    uint8_t u8 = 0x12;
    uint16_t u16 = 0x1234;
    uint32_t u32 = 0x12345678;
    uint64_t u64 = 0x12345678abcdef12;

    int8_t i8 = -u8;
    int16_t i16 = -u16;
    int32_t i32 = -u32;
    int64_t i64 = -u64;

    ss.put_u8(u8);
    ss.put_u16(u16);
    ss.put_u32(u32);
    ss.put_u64(u64);

    ss.put_i8(i8);
    ss.put_i16(i16);
    ss.put_i32(i32);
    ss.put_i64(i64);

    std::string helloworld = "helloworld";

    // Put string without length.
    ss.put_raw(helloworld.data(), helloworld.size());

    // Put C-style string.
    ss.put_cstr(helloworld.c_str());

    // Put string with length.
    ss.put_bytes(helloworld.data(), helloworld.size());

    // Put string directly (the same as `put_bytes`).
    ss.put_str(helloworld);

    // Other buffer containing string.
    ptr<buffer> hw_buf = buffer::alloc(helloworld.size() + sizeof(uint32_t));
    buffer_serializer bs_hw_buf(hw_buf, endian);
    bs_hw_buf.put_str(helloworld);
    ss.put_buffer(*hw_buf);

    // Out-of-bound write.
    {
        bool got_exception = false;
        try {
            char dummy[256];
            ss.put_bytes(dummy, 256);
        } catch (...) {
            got_exception = true;
        }
        CHK_TRUE(got_exception);
    }

    // Original buffer's cursor should not move.
    CHK_Z( buf->pos() );

    buffer_serializer ss_read(buf, endian);

    CHK_EQ( u8, ss_read.get_u8() );
    CHK_EQ( u16, ss_read.get_u16() );
    CHK_EQ( u32, ss_read.get_u32() );
    CHK_EQ( u64, ss_read.get_u64() );

    CHK_EQ( i8, ss_read.get_i8() );
    CHK_EQ( i16, ss_read.get_i16() );
    CHK_EQ( i32, ss_read.get_i32() );
    CHK_EQ( i64, ss_read.get_i64() );

    // Binary without length.
    void* ptr_read = nullptr;
    ptr_read = ss_read.get_raw(helloworld.size());
    CHK_EQ( helloworld, std::string((const char*)ptr_read, helloworld.size()) );

    // C-style string.
    const char* hw_cstr = ss_read.get_cstr();
    CHK_EQ( helloworld, std::string(hw_cstr) );

    // Binary with length.
    size_t len = 0;
    ptr_read = ss_read.get_bytes(len);
    CHK_EQ( helloworld.size(), len );
    CHK_EQ( helloworld, std::string((const char*)ptr_read, len) );

    // String.
    std::string str_read = ss_read.get_str();
    CHK_EQ( helloworld, str_read );

    // Buffer.
    ptr<buffer> hw_buf_read =
        buffer::alloc(helloworld.size() + sizeof(uint32_t));
    ss_read.get_buffer(hw_buf_read);
    buffer_serializer bs_hw_buf_read(hw_buf_read, endian);
    str_read = bs_hw_buf_read.get_str();
    CHK_EQ( helloworld, str_read );

    // Out-of-bound read.
    {
        bool got_exception = false;
        try {
            void* dummy = ss.get_raw(256);
            (void)dummy;
        } catch (...) {
            got_exception = true;
        }
        CHK_TRUE(got_exception);
    }

    // Position of both serializers should be the same.
    CHK_EQ( ss.pos(), ss_read.pos() );

    // Original buffer's cursor should not move.
    CHK_Z( buf->pos() );

    return 0;
}

}  // namespace buffer_test;
using namespace buffer_test;

int main(int argc, char** argv) {
    TestSuite ts(argc, argv);

    ts.options.printTestMessage = false;

    ts.doTest( "buffer basic test",
               buffer_basic_test,
               TestRange<size_t>( {1024, 0x8000, 0x10000} ) );

    ts.doTest( "buffer serializer test",
               buffer_serializer_test,
               TestRange<bool>( {true, false} ) );

    return 0;
}


