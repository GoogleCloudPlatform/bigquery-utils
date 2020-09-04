load("@google_bazel_common//tools/jarjar:jarjar.bzl", "jarjar_library")
#load("@google_bazel_common//tools/javadoc:javadoc.bzl", "javadoc_library")
#load("//tools:maven.bzl", "pom_file")

CLIENT_SRCS = [
    "Client.java",
    "Token.java",
    "ZetaSqlHelper.java",
    "QueryLocationRange.java",
    "QueryFunctionRange.java",
]

# Import Lombok
java_library(
    name = "lombok",
    exported_plugins = [
        ":lombok_plugin",
    ],
    exports = [
        "@maven//:org_projectlombok_lombok",
    ],
)

# Import Lombok
java_plugin(
    name = "lombok_plugin",
    generates_api = 1,
    processor_class = "lombok.launch.AnnotationProcessorHider$AnnotationProcessor",
    deps = [
        "@maven//:org_projectlombok_lombok",
    ],
)

java_library(
    name = "client",
    srcs = CLIENT_SRCS,
    runtime_deps = [
        "@maven//:io_grpc_grpc_netty_shaded",
    ],
    deps = [
        ":lombok",
        ":service_provider",
        "//zetasql_helper/local_service:local_service_java_grpc",
        "//zetasql_helper/local_service:local_service_java_proto",
        "//zetasql_helper/token:parse_token_java_proto",
        "@com_google_api_grpc_proto_google_common_protos//jar",
        "@com_google_protobuf//:protobuf_java",
        "@com_google_protobuf//:protobuf_java_util",
        "@com_google_zetasql//zetasql/public:parse_location_range_java_proto",
        "@io_grpc_grpc_core//jar",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:org_projectlombok_lombok",
    ],
)

java_library(
    name = "service_provider",
    srcs = [
        "LocalServiceDockerProvider.java",
        "LocalServiceException.java",
        "LocalServiceProvider.java",
    ],
    resource_strip_prefix = "java/resources/",
    resources = [
        "//java/resources",
    ],
    deps = [
        "@io_grpc_grpc_core//jar",
        "@maven//:com_github_docker_java_docker_java_api",
        "@maven//:com_github_docker_java_docker_java_core",
        "@maven//:com_github_docker_java_docker_java_transport",
        "@maven//:com_github_docker_java_docker_java_transport_httpclient5",
    ],
)

java_binary(
    name = "run_client",
    srcs = ["Main.java"],
    main_class = "com.google.bigquery.utils.zetasqlhelper.Main",
    runtime_deps = [
        "@maven//:io_grpc_grpc_netty_shaded",
    ],
    deps = [
        ":client",
        ":service_provider",
        "@io_grpc_grpc_core//jar",
    ],
)

# Pack the client into a jar
jarjar_library(
    name = "zetasql_helper_client",
    jars = [
        ":client",
        ":service_provider",
        "//zetasql_helper/local_service:local_service_java_grpc",
        "//zetasql_helper/local_service:local_service_java_proto",
        "//zetasql_helper/token:parse_token_java_proto",
    ],
    rules = [
        "zap com.google.protobuf.**",
    ],
)
