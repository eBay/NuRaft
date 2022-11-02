Custom Metadata For Each Message
--------------------------------

NuRaft provides an ability to ship custom metadata for each message and response that can be used for your own verification. When you set [`asio_options`](../include/libnuraft/asio_service_options.hxx), there are a few options for it:
```C++
asio_service::options asio_opt;
asio_opt.write_req_meta_  = my_write_req_meta;
asio_opt.read_req_meta_   = my_read_req_meta;
asio_opt.write_resp_meta_ = my_write_resp_meta;
asio_opt.read_resp_meta_  = my_read_resp_meta;
```

* `write_req_meta_`: return custom metadata to be shipped with the Raft message.
```c++
std::string my_write_req_meta(const asio_service::meta_cb_params& params) {
    return "my req metadata";
}
```

* `read_req_meta_`: read the custom metadata shipped with each Raft message, and return a boolean flag to decide to accept the message.
```c++
bool my_read_req_meta(const asio_service::meta_cb_params& params,
                      const std::string& meta) {
    return (meta == "my req metadata");
}
```

* `write_resp_meta_`: return custom metadata to be shipped with the Raft message response.
```c++
std::string my_write_resp_meta(const asio_service::meta_cb_params& params) {
    return "my resp metadata";
}
```

* `read_resp_meta_`: read the custom metadata shipped with the Raft message response, and return a boolean flag to decide to accept the response.
```c++
bool my_read_resp_meta(const asio_service::meta_cb_params& params,
                       const std::string& meta) {
    return (meta == "my resp metadata");
}
```

If `read_req_meta_` or `read_resp_meta_` returns `false`, the message or response is discarded immediately; thus it behaves the same as if it is not received.
