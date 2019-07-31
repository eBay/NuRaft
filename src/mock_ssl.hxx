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

class mock_ssl_context {
public:
    enum type {
        sslv23 = 0,
    };

    mock_ssl_context(type t) {}
};

class mock_ssl_socket {
public:
    using lowest_layer_type = asio::ip::tcp::socket;

    mock_ssl_socket(asio::ip::tcp::socket& tcp_socket,
                    mock_ssl_context& context)
        : socket_(tcp_socket)
        , context_(context)
        {}

    lowest_layer_type& lowest_layer() { return socket_; }

    template<typename A, typename B>
    void async_read_some(A a, B b) {}

    template<typename A, typename B>
    void async_write_some(A a, B b) {}

    asio::ip::tcp::socket& socket_;
    mock_ssl_context& context_;
};

