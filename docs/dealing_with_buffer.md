Dealing with Buffer
---

[`buffer`](../include/libnuraft/buffer.hxx) class instance just points to a raw memory blob. At the beginning of each memory blob, a few bytes are reserved for metadata: 4 bytes if buffer size is less than 32KB and 8 bytes otherwise. User data section starts right after that, thus you should write your data starting from there.

You can use `alloc()` API to allocate memory:
```C++
ptr<buffer> b = buffer::alloc( size_to_allocate );
```

You can get the starting point of user section by using `data()` API:
```C++
void* ptr = (void*)b->data();
```

Each buffer instance has a cursor pointing to its current internal position (initially 0), you can get or set the position:
```C++
size_t current_position = b->pos();
...
b->pos( new_position );
```

If you want to get the starting point of user section regardless of the current position, use `data_begin()` API:
```C++
void* ptr_begin = (void*)b->data_begin();
```

Buffer Serializer
---
`buffer` itself has a few APIs to get and put data, but we do not recommend using those APIs, since they change the internal position of the buffer, which blocks concurrent reads and also is mistake-prone if you forget to reset the position.

Instead, you can use [`buffer_serializer`](../include/libnuraft/buffer_serializer.hxx):
```C++
ptr<buffer> b = buffer::alloc( size_to_allocate );
buffer_serializer s(b);
// ... get or put APIs ...
```
`buffer_serializer` allows concurrent access to the same `buffer` instance.

We provide endianness options, and it is little endian by default.
