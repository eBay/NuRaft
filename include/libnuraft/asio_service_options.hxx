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

#include <functional>
#include <string>
#include <system_error>


typedef struct ssl_ctx_st SSL_CTX;

namespace nuraft {

/**
 * Parameters for meta callback functions in `options`.
 */
struct asio_service_meta_cb_params {
    asio_service_meta_cb_params(int m = 0,
                                int s = 0,
                                int d = 0,
                                uint64_t t = 0,
                                uint64_t lt = 0,
                                uint64_t li = 0,
                                uint64_t ci = 0)
        : msg_type_(m), src_id_(s), dst_id_(d)
        , term_(t), log_term_(lt), log_idx_(li), commit_idx_(ci)
        {}

    // Type of request.
    int msg_type_;

    // Source server ID that sends request.
    int src_id_;

    // Destination server ID that sends response.
    int dst_id_;

    // Term of source server.
    uint64_t term_;

    // Term of the corresponding log.
    uint64_t log_term_;

    // Log index number.
    uint64_t log_idx_;

    // Last committed index number.
    uint64_t commit_idx_;
};

/**
 * Response callback function for customer resolvers.
 */
using asio_service_custom_resolver_response =
    std::function< void(const std::string&, const std::string&, std::error_code) >;

/**
 * Options used for initialization of Asio service.
 */
struct asio_service_options {
    asio_service_options()
        : thread_pool_size_(0)
        , worker_start_(nullptr)
        , worker_stop_(nullptr)
        , enable_ssl_(false)
        , skip_verification_(false)
        , write_req_meta_(nullptr)
        , read_req_meta_(nullptr)
        , invoke_req_cb_on_empty_meta_(true)
        , write_resp_meta_(nullptr)
        , read_resp_meta_(nullptr)
        , invoke_resp_cb_on_empty_meta_(true)
        , verify_sn_(nullptr)
        , custom_resolver_(nullptr)
        , replicate_log_timestamp_(false)
        {}

    /**
     * Number of ASIO worker threads.
     * If zero, it will be automatically set to number of cores.
     */
    size_t thread_pool_size_;

    /**
     * Lifecycle callback function on worker thread start.
     */
    std::function< void(uint32_t) > worker_start_;

    /**
     * Lifecycle callback function on worker thread stop.
     */
    std::function< void(uint32_t) > worker_stop_;

    /**
     * If `true`, enable SSL/TLS secure connection.
     */
    bool enable_ssl_;

    /**
     * If `true`, skip certificate verification.
     */
    bool skip_verification_;

    /**
     * Path to server certificate file.
     */
    std::string server_cert_file_;

    /**
     * Path to server key file.
     */
    std::string server_key_file_;

    /**
     * Path to root certificate file.
     */
    std::string root_cert_file_;

    /**
     * Callback function for writing Raft RPC request metadata.
     */
    std::function< std::string(const asio_service_meta_cb_params&) > write_req_meta_;

    /**
     * Callback function for reading and verifying Raft RPC request metadata.
     * If it returns `false`, the request will be discarded.
     */
    std::function< bool( const asio_service_meta_cb_params&,
                         const std::string& ) > read_req_meta_;

    /**
     * If `true`, it will invoke `read_req_meta_` even though
     * the received meta is empty.
     */
    bool invoke_req_cb_on_empty_meta_;

    /**
     * Callback function for writing Raft RPC response metadata.
     */
    std::function< std::string(const asio_service_meta_cb_params&) > write_resp_meta_;

    /**
     * Callback function for reading and verifying Raft RPC response metadata.
     * If it returns false, the response will be ignored.
     */
    std::function< bool( const asio_service_meta_cb_params&,
                         const std::string& ) > read_resp_meta_;

    /**
     * If `true`, it will invoke `read_resp_meta_` even though
     * the received meta is empty.
     */
    bool invoke_resp_cb_on_empty_meta_;

    /**
     * Callback function for verifying certificate subject name.
     * If not given, subject name will not be verified.
     */
    std::function< bool(const std::string&) > verify_sn_;

    // If true will try to load CA from default path
    // call (SSL_CTX_set_default_verify_paths)
    bool load_default_ca_file_;

    // If set, will use SSL_CTX povided by callback
    std::function<SSL_CTX* (void)> ssl_context_provider_server_;
    std::function<SSL_CTX* (void)> ssl_context_provider_client_;

    /**
     * Custom IP address resolver. If given, it will be invoked
     * before the connection is established.
     *
     * If you want to selectively bypass some hosts, just pass the given
     * host and port to the response function as they are.
     */
    std::function< void( const std::string&,
                         const std::string&,
                         asio_service_custom_resolver_response ) > custom_resolver_;

    /**
     * If `true`, each log entry will contain timestamp when it was generated
     * by the leader, and those timestamps will be replicated to all followers
     * so that they will see the same timestamp for the same log entry.
     *
     * To support this feature, the log store implementation should be able to
     * restore the timestamp when it reads log entries.
     *
     * This feature is not backward compatible. To enable this feature, there
     * should not be any member running with old version before supprting
     * this flag.
     */
    bool replicate_log_timestamp_;
};

}

