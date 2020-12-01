load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@io_grpc_grpc_java//:java_grpc_library.bzl", "java_grpc_library")

package(
    default_visibility = ["//visibility:public"],
)

proto_library(
    name = "local_service_proto",
    srcs = ["local_service.proto"],
    deps = [
        "//zetasql_helper/token:parse_token_proto",
        "//zetasql_helper/scanner:function_range_proto",
        "@com_google_zetasql//zetasql/public:parse_location_range_proto",
    ],
)

cc_proto_library(
    name = "local_service_cc_proto",
    deps = [":local_service_proto"],
)

cc_grpc_library(
    name = "local_service_cc_grpc",
    srcs = [":local_service_proto"],
    grpc_only = True,
    deps = [":local_service_cc_proto"],
)

cc_library(
    name = "local_service",
    srcs = ["local_service.cc"],
    hdrs = ["local_service.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":local_service_cc_proto",
        # dep regarding implementation
        "//zetasql_helper/token",
        "//zetasql_helper/scanner:locate_table",
        "//zetasql_helper/scanner:extract_function",
        "//zetasql_helper/fixer:fix_duplicate_columns",
        "//zetasql_helper/fixer:fix_column_not_grouped",
        "@com_google_zetasql//zetasql/parser",
    ],
)

cc_library(
    name = "local_service_grpc",
    srcs = ["local_service_grpc.cc"],
    hdrs = ["local_service_grpc.h"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":local_service_cc_grpc",
        ":local_service_cc_proto",
        "@com_github_grpc_grpc//:grpc++",
        # dep regarding implementation
        ":local_service"
    ],
)

java_proto_library(
    name = "local_service_java_proto",
    deps = [":local_service_proto"],
)

java_grpc_library(
    name = "local_service_java_grpc",
    srcs = [":local_service_proto"],
    deps = [
        ":local_service_java_proto",
    ],
)

cc_binary(
    name = "run_server",
    srcs = ["run_server.cc"],
    deps = [
        ":local_service_grpc",
        "@com_github_grpc_grpc//:grpc++",
        "@com_github_grpc_grpc//:grpc++_reflection",
    ],
)

cc_test(
    name = "local_service_test",
    size = "small",
    srcs = ["local_service_test.cc"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":local_service_grpc",
        "@com_google_googletest//:gtest_main",
    ],
)
