package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "token",
    srcs = ["token.cc"],
    hdrs = ["token.h"],
    deps = [
        "@com_google_zetasql//zetasql/public:parse_helpers",
        ":parse_token_cc_proto",
    ],
)

proto_library(
    name = "parse_token_proto",
    srcs = ["parse_token.proto"],
    deps = [
        "@com_google_zetasql//zetasql/public:parse_location_range_proto",
        "@com_google_zetasql//zetasql/public:value_proto",
    ],
)

cc_proto_library(
    name = "parse_token_cc_proto",
    deps = [":parse_token_proto"],

)

java_proto_library(
    name = "parse_token_java_proto",
    deps = [":parse_token_proto"],

)

cc_test(
    name = "token_test",
    size = "small",
    srcs = ["token_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":token",
        "@com_google_googletest//:gtest_main",
    ],
)