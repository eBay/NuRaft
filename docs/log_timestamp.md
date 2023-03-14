Replication Log Timestamp
-------------------------

NuRaft provides an option to generate the timestamp of each log automatically. Whenever a leader receives an `append_entries` request, it generates logs for the replication. With this option, those logs will contain a timestamp of the time they were generated. Such timestamps will be useful when you want to measure the time gap between two particular logs, in order to estimate the freshness of data.

It is disabled by default, and you can enable it by setting `replicate_log_timestamp_` in [`asio_options`](../include/libnuraft/asio_service_options.hxx) to `true`. [`log_entry`](../include/libnuraft/log_entry.hxx) has an attribute `timestamp_us_` and the timestamp (in microseconds) is automatically set by NuRaft. Your log store implementation should properly store the timestamp and be able to restore it when NuRaft wants to retrieve the log entry. [`log_entry`](../include/libnuraft/log_entry.hxx) provides `get_timestamp` and `set_timestamp` APIs to get and set timestamps.

Note that this feature is not backward compatible; NuRaft instance older than commit [8fdf1e4](https://github.com/eBay/NuRaft/commit/8fdf1e4a41d0db5408cad86eac359f579311d760) will not be able to understand the message if the option is set to `true`. Thus, if you want to enable this feature, ensure all cluster members are running with the newer version.

