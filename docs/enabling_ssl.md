Enabling SSL/TLS
----------------

When you set [`asio_options`](../include/libnuraft/asio_service_options.hxx), there are a few options for enabling SSL/TLS:
```C++
asio_service::options asio_opt;
asio_opt.enable_ssl_        = true;
asio_opt.verify_sn_         = my_verification_function;
asio_opt.server_cert_file_  = "./cert.pem";
asio_opt.root_cert_file_    = "./root_ca.pem";
asio_opt.server_key_file_   = "./key.pem";
```

Then Raft server will use SSL/TLS when it establishes connection between them. Note that Raft is a peer-to-peer protocol, thus each node can be both client and server at the same time.

Server certificate, root certificate, and key file should be in [PEM format](https://en.wikipedia.org/wiki/Privacy-Enhanced_Mail).

`verify_sn_` callback function is for client (i.e., mostly when the node is leader) to verify the server certificate, by using common name or others. Your callback function should be defined as follows:
```C++
bool my_verification_function(const std::string& sn) {
    // Verify given string `sn`, return `true` if it is valid.
}
```

`sn` will be like this:
```
"/C=AB/ST=CD/L=EFG/O=ORG/CN=localhost"
```

If the given ceritifcate is invalid, this function can return `false`, then the node will not accept the connection. Otherwise, just return `true`.
