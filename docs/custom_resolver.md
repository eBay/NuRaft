Custom Resolver
---------------

You can attach your custom resolver that can be used when establishing new connections. When you set [`asio_options`](../include/libnuraft/asio_service_options.hxx), there is an option named `custom_resolver_`:
```C++
std::function< void( const std::string&,
                     const std::string&,
                     asio_service_custom_resolver_response ) > custom_resolver_;
```
The first parameter contains the host name to resolve, and the second has the port number. The third parameter is a callback function to be invoked when your resolver finishes its job.

The type of the callback function is as follows:
```c++
using asio_service_custom_resolver_response =
    std::function< void(const std::string&, const std::string&, std::error_code) >;
```
You need to pass the resolved IP address (or the host name that the default resolver can understand) to the first parameter and the port number to the second one. The third parameter should contain an error code if resolving the host name fails.

This is a very simple example of resolving `"my_custom_localhost"`:
```c++
asio_service::options asio_opt;
asio_opt.custom_resolver_ =
    [](const std::string& host,
       const std::string& port,
       asio_service_custom_resolver_response when_done) {
        if (host == "my_custom_localhost") {
            when_done("127.0.0.1", port, std::error_code());
        }
        // Otherwise: pass through. The default resolver will do the rest.
        when_done(host, port, std::error_code());
    };
```
