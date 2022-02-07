load("@rules_cc//cc:defs.bzl", "cc_library")
cc_library(
    name = "asio",
    hdrs = glob(["include/**/*.hpp"]) + glob(["include/**/*.ipp"]),
    defines = ["ASIO_STANDALONE"],
    visibility = ["//visibility:public"],
    strip_include_prefix = "//include",
)