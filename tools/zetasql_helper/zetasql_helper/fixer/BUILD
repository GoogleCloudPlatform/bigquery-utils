package(
    default_visibility = ["//visibility:public"],
)

cc_library(
    name = "fix_column_not_grouped",
    srcs = ["fix_column_not_grouped.cc"],
    hdrs = ["fix_column_not_grouped.h"],
    deps = [
        "//zetasql_helper/util:util",
        "@com_google_zetasql//zetasql/parser",
        "@com_google_zetasql//zetasql/parser:parse_tree",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:parse_helpers",
    ],
)

cc_library(
    name = "fix_duplicate_columns",
    srcs = ["fix_duplicate_columns.cc"],
    hdrs = ["fix_duplicate_columns.h"],
    deps = [
        "//zetasql_helper/util:util",
        "@com_google_zetasql//zetasql/parser",
        "@com_google_zetasql//zetasql/parser:parse_tree",
        "@com_google_zetasql//zetasql/public:analyzer",
        "@com_google_zetasql//zetasql/public:parse_helpers",
    ],
)

cc_test(
    name = "fixer_test",
    size = "small",
    srcs = ["fixer_test.cpp"],
    copts = ["-Wno-sign-compare"],
    deps = [
        ":fix_column_not_grouped",
        ":fix_duplicate_columns",
        "@com_google_googletest//:gtest_main",
    ],
)
