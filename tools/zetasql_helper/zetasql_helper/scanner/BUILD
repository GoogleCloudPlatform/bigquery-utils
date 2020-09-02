package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "extract_function",
    srcs = ["extract_function.cc"],
    hdrs = ["extract_function.h"],
    deps = [
        "@com_google_zetasql//zetasql/parser",
        "@com_google_zetasql//zetasql/parser:parse_tree",
        "@com_google_zetasql//zetasql/public:analyzer",
        ":function_range_cc_proto",
        "//zetasql_helper/util:util",
    ],
)

cc_library(
    name = "locate_table",
    srcs = ["locate_table.cc"],
    hdrs = ["locate_table.h"],
    deps = [
        "@com_google_zetasql//zetasql/parser",
        "@com_google_zetasql//zetasql/parser:parse_tree",
        "@com_google_zetasql//zetasql/public:analyzer",
        "//zetasql_helper/util:util",
    ],
)

proto_library(
    name = "function_range_proto",
    srcs = ["function_range.proto"],
    deps = [
        "@com_google_zetasql//zetasql/public:parse_location_range_proto",
    ],
)

cc_proto_library(
    name = "function_range_cc_proto",
    deps = [":function_range_proto"],
)

java_proto_library(
    name = "function_range_java_proto",
    deps = [":function_range_proto"],
)

cc_test(
    name = "scanner_test",
    size = "small",
    srcs = ["scanner_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":locate_table",
        ":extract_function",
        "@com_google_googletest//:gtest_main",
    ],
)