load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:utils.bzl", "maybe")

def nuraft_deps():
    maybe(
        http_archive,
        name = "asio",
        sha256 = "4cd5cd0ad97e752a4075f02778732a3737b587f5eeefab59cd98dc43b0dcadb3",
        urls = [
            "https://shutian.oss-cn-hangzhou.aliyuncs.com/cdn/asio/asio-1.20.0.tar.gz",
        ],
        strip_prefix = "asio-1.20.0",
        build_file = "@com_github_ebay_nuraft//third_party:asio.BUILD",
    )

    maybe(
        http_archive,
        name = "boringssl",
        # Use github mirror instead of https://boringssl.googlesource.com/boringssl
        # to obtain a boringssl archive with consistent sha256
        sha256 = "534fa658bd845fd974b50b10f444d392dfd0d93768c4a51b61263fd37d851c40",
        strip_prefix = "boringssl-b9232f9e27e5668bc0414879dcdedb2a59ea75f2",
        urls = [
            "https://shutian.oss-cn-hangzhou.aliyuncs.com/cdn/boringssl/boringssl-b9232f9e27e5668bc0414879dcdedb2a59ea75f2.tar.gz",
            "https://storage.googleapis.com/grpc-bazel-mirror/github.com/google/boringssl/archive/b9232f9e27e5668bc0414879dcdedb2a59ea75f2.tar.gz",
            "https://github.com/google/boringssl/archive/b9232f9e27e5668bc0414879dcdedb2a59ea75f2.tar.gz",
        ],
    )