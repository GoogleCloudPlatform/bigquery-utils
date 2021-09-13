// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const {generate_udf_test} = unit_test_utils;

generate_udf_test("int", [
    {
        inputs: [`CAST(-1 AS STRING)`],
        expected_output: `CAST(-1 AS INT64)`
    },
]);
generate_udf_test("int", [
    {
        inputs: [`CAST(1 AS INT64)`],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [`CAST(7 AS INT64)`],
        expected_output: `CAST(7 AS INT64)`
    },
]);
generate_udf_test("int", [
    {
        inputs: [`CAST(2.5 AS FLOAT64)`],
        expected_output: `CAST(2 AS INT64)`
    },
    {
        inputs: [`CAST(7.8 AS FLOAT64)`],
        expected_output: `CAST(7 AS INT64)`
    },
]);
generate_udf_test("json_typeof", [
    {
        inputs: [`'""'`],
        expected_output: `CAST("string" AS STRING)`
    },
    {
        inputs: [`'"test"'`],
        expected_output: `CAST("string" AS STRING)`
    },
    {
        inputs: [`"true"`],
        expected_output: `"boolean"`
    },
    {
        inputs: [`"false"`],
        expected_output: `"boolean"`
    },
    {
        inputs: [`"null"`],
        expected_output: `"null"`
    },
    {
        inputs: [`"0"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"1"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"-1"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"0.0"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"1.0"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"-1.0"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"1e1"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"-1e1"`],
        expected_output: `"number"`
    },
    {
        inputs: [`"[]"`],
        expected_output: `"array"`
    },
    {
        inputs: [`"[1, 2, 3]"`],
        expected_output: `"array"`
    },
    {
        inputs: [`"{}"`],
        expected_output: `"object"`
    },
    {
        inputs: [`'{"foo":"bar"}'`],
        expected_output: `"object"`
    },
    {
        inputs: [`""`],
        expected_output: `NULL`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`b"test"`],
        expected_output: `"BYTES"`
    },
    {
        inputs: [`b""`],
        expected_output: `"BYTES"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`"test"`],
        expected_output: `"STRING"`
    },
    {
        inputs: [`""`],
        expected_output: `"STRING"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`TRUE`],
        expected_output: `"BOOL"`
    },
    {
        inputs: [`FALSE`],
        expected_output: `"BOOL"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`NULL`],
        expected_output: `"NULL"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`1`],
        expected_output: `"INT64"`
    },
    {
        inputs: [`-1`],
        expected_output: `"INT64"`
    },
    {
        inputs: [`0`],
        expected_output: `"INT64"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`0.0`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`-1.0`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`1.0`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`+123e45`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`-123e45`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`12e345`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`-12e345`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`CAST("inf" AS FLOAT64)`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`CAST("-inf" AS FLOAT64)`],
        expected_output: `"FLOAT64"`
    },
    {
        inputs: [`CAST("nan" AS FLOAT64)`],
        expected_output: `"FLOAT64"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`DATE "1970-01-01"`],
        expected_output: `"DATE"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`DATETIME "1970-01-01T00:00:00"`],
        expected_output: `"DATETIME"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`TIME "00:00:00"`],
        expected_output: `"TIME"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`TIMESTAMP "1970-01-01T00:00:00Z"`],
        expected_output: `"TIMESTAMP"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`STRUCT()`],
        expected_output: `"STRUCT"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`STRUCT(1)`],
        expected_output: `"STRUCT"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`STRUCT(1, 2, 3)`],
        expected_output: `"STRUCT"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`STRUCT<a INT64, b INT64, c INT64>(1, 2, 3)`],
        expected_output: `"STRUCT"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`[]`],
        expected_output: `"ARRAY"`
    },
    {
        inputs: [`[1, 2, 3]`],
        expected_output: `"ARRAY"`
    },
    {
        inputs: [`ARRAY<INT64>[1, 2, 3]`],
        expected_output: `"ARRAY"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`ST_GEOGPOINT(0, 0)`],
        expected_output: `"GEOGRAPHY"`
    },
]);
generate_udf_test("typeof", [
    {
        inputs: [`NUMERIC "0"`],
        expected_output: `"NUMERIC"`
    },
    {
        inputs: [`NUMERIC "1"`],
        expected_output: `"NUMERIC"`
    },
    {
        inputs: [`NUMERIC "-1"`],
        expected_output: `"NUMERIC"`
    },
]);
generate_udf_test("url_parse", [
    {
        inputs: [
            `CAST("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("HOST" AS STRING)`
        ],
        expected_output: `CAST("facebook.com" AS STRING)`
    },
    {
        inputs: [
            `CAST("rpc://facebook.com/" AS STRING)`,
            `CAST("HOST" AS STRING)`
        ],
        expected_output: `CAST("facebook.com" AS STRING)`
    },
    {
        inputs: [
            `CAST("subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("HOST" AS STRING)`
        ],
        expected_output: `CAST("subdomain.facebook.com" AS STRING)`
    },
    {
        inputs: [
            `CAST("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("PATH" AS STRING)`
        ],
        expected_output: `CAST("path1/p.php" AS STRING)`
    },
    {
        inputs: [
            `CAST("subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("PATH" AS STRING)`
        ],
        expected_output: `CAST("path1/p.php" AS STRING)`
    },
    {
        inputs: [
            `CAST("rpc://facebook.com/" AS STRING)`,
            `CAST("PATH" AS STRING)`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `CAST("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("QUERY" AS STRING)`
        ],
        expected_output: `CAST("k1=v1&k2=v2#Ref1" AS STRING)`
    },
    {
        inputs: [
            `CAST("rpc://facebook.com/" AS STRING)`,
            `CAST("QUERY" AS STRING)`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `CAST("subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("QUERY" AS STRING)`
        ],
        expected_output: `CAST("k1=v1&k2=v2#Ref1" AS STRING)`
    },
    {
        inputs: [
            `CAST("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("REF" AS STRING)`
        ],
        expected_output: `CAST("Ref1" AS STRING)`
    },
    {
        inputs: [
            `CAST("rpc://facebook.com/" AS STRING)`,
            `CAST("REF" AS STRING)`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `CAST("subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("REF" AS STRING)`
        ],
        expected_output: `CAST("Ref1" AS STRING)`
    },
    {
        inputs: [
            `CAST("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("PROTOCOL" AS STRING)`
        ],
        expected_output: `CAST("http" AS STRING)`
    },
    {
        inputs: [
            `CAST("rpc://facebook.com/" AS STRING)`,
            `CAST("PROTOCOL" AS STRING)`
        ],
        expected_output: `CAST("rpc" AS STRING)`
    },
    {
        inputs: [
            `CAST("subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1" AS STRING)`,
            `CAST("PROTOCOL" AS STRING)`
        ],
        expected_output: `NULL`
    },
]);
generate_udf_test("percentage_change", [
    {
        inputs: [
            `CAST(0.2 AS FLOAT64)`,
            `CAST(0.4 AS FLOAT64)`
        ],
        expected_output: `CAST(1.0 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(5 AS NUMERIC)`,
            `CAST(15 AS NUMERIC)`
        ],
        expected_output: `CAST(2.0 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(100 AS INT64)`,
            `CAST(50 AS INT64)`
        ],
        expected_output: `CAST(-0.5 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(-20 AS INT64)`,
            `CAST(-45 AS INT64)`
        ],
        expected_output: `CAST(-1.25 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(0 AS INT64)`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(10 AS INT64)`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(10 AS INT64)`,
            `CAST(NULL AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(0 AS FLOAT64)`,
            `CAST(10 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(NULL AS INT64)`,
            `CAST(10 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
]);
generate_udf_test("percentage_difference", [
    {
        inputs: [
            `CAST(0.22222222 AS FLOAT64)`,
            `CAST(0.88888888 AS FLOAT64)`
        ],
        expected_output: `CAST(1.2 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(0.2 AS NUMERIC)`,
            `CAST(0.8 AS NUMERIC)`
        ],
        expected_output: `CAST(1.2 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(2 AS INT64)`,
            `CAST(8 AS INT64)`
        ],
        expected_output: `CAST(1.2 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(100 AS INT64)`,
            `CAST(200 AS INT64)`
        ],
        expected_output: `CAST(0.6667 AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(-2 AS INT64)`,
            `CAST(8 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(2 AS INT64)`,
            `CAST(-8 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(0 AS INT64)`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(0 AS INT64)`,
            `CAST(100 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(100 AS INT64)`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `CAST(NULL AS FLOAT64)`
    },
    {
        inputs: [
            `CAST(1.0 AS FLOAT64)`,
            `CAST(1000000000 AS INT64)`
        ],
        expected_output: `CAST(2.0 AS FLOAT64)`
    },
]);
generate_udf_test("linear_interpolate", [{
    inputs: [
        `CAST(2 AS INT64)`,
        `STRUCT(CAST(1 AS INT64) AS x, CAST(1.0 AS FLOAT64) AS y)`,
        `STRUCT(CAST(3 AS INT64) AS x, CAST(3.0 AS FLOAT64) AS y)`,
    ],
    "expected_output": `CAST(2.0 AS FLOAT64)`,
}, {
    inputs: [
        `CAST(3 AS INT64)`,
        `STRUCT(CAST(1 AS INT64) AS x, CAST(1.0 AS FLOAT64) AS y)`,
        `STRUCT(CAST(4 AS INT64) AS x, CAST(4.0 AS FLOAT64) AS y)`,
    ],
    "expected_output": `CAST(3.0 AS FLOAT64)`,
},
]);
generate_udf_test("ts_linear_interpolate", [{
    inputs: [
        `CAST("2020-01-01 00:15:00" AS TIMESTAMP)`,
        `STRUCT(CAST("2020-01-01 00:00:00" AS TIMESTAMP) AS x, CAST(1.0 AS FLOAT64))`,
        `STRUCT(CAST("2020-01-01 00:30:00" AS TIMESTAMP) AS x, CAST(3.0 AS FLOAT64))`,
    ],
    "expected_output": `CAST(2.0 AS FLOAT64)`,
}, {
    inputs: [
        `CAST("2020-01-01 00:15:00" AS TIMESTAMP)`,
        `STRUCT(CAST("2020-01-01 00:00:00" AS TIMESTAMP) AS x, CAST(1.0 AS FLOAT64))`,
        `STRUCT(CAST("2020-01-01 02:30:00" AS TIMESTAMP) AS x, CAST(3.0 AS FLOAT64))`,
    ],
    "expected_output": `CAST(1.2 AS FLOAT64)`,
},
]);
generate_udf_test("ts_tumble", [
    {
        inputs: [
            `CAST("2020-01-01 00:17:30" AS TIMESTAMP)`,
            `CAST(900 AS INT64)`
        ],
        expected_output: `CAST("2020-01-01 00:15:00" AS TIMESTAMP)`
    },
    {
        inputs: [
            `CAST("2020-01-01 00:17:30" AS TIMESTAMP)`,
            `CAST(600 AS INT64)`
        ],
        expected_output: `CAST("2020-01-01 00:10:00" AS TIMESTAMP)`
    },
    {
        inputs: [
            `CAST("2020-01-01 00:17:30" AS TIMESTAMP)`,
            `CAST(300 AS INT64)`
        ],
        expected_output: `CAST("2020-01-01 00:15:00" AS TIMESTAMP)`
    },
    {
        inputs: [
            `CAST("2020-01-01 00:17:30" AS TIMESTAMP)`,
            `CAST(60 AS INT64)`
        ],
        expected_output: `CAST("2020-01-01 00:17:00" AS TIMESTAMP)`
    },
    {
        inputs: [
            `CAST("2020-01-01 00:17:30" AS TIMESTAMP)`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `NULL`
    },
]);
generate_udf_test("ts_gen_keyed_timestamps", [
    {
        inputs: [
            `ARRAY<STRING>["abc"]`,
            `CAST(60 AS INT64)`,
            `CAST("2020-01-01 00:30:00" AS TIMESTAMP)`,
            `CAST("2020-01-01 00:31:00" AS TIMESTAMP)`
        ],
        expected_output: `([STRUCT(CAST("abc" AS STRING) AS series_key, CAST("2020-01-01 00:30:00" AS TIMESTAMP) AS tumble_val), STRUCT(CAST("abc" AS STRING) AS series_key, CAST("2020-01-01 00:31:00" AS TIMESTAMP) AS tumble_val)])`
    },
    {
        inputs: [
            `ARRAY<STRING>["abc", "def"]`,
            `CAST(60 AS INT64)`,
            `CAST("2020-01-01 00:30:00" AS TIMESTAMP)`,
            `CAST("2020-01-01 00:30:30" AS TIMESTAMP)`
        ],
        expected_output: `([STRUCT(CAST("abc" AS STRING) AS series_key, CAST("2020-01-01 00:30:00" AS TIMESTAMP) AS tumble_val), STRUCT(CAST("def" AS STRING) AS series_key, CAST("2020-01-01 00:30:00" AS TIMESTAMP) AS tumble_val)])`
    },
]);
generate_udf_test("ts_session_group", [
    {
        inputs: [
            `CAST("2020-01-01 01:04:59 UTC" AS TIMESTAMP)`,
            `NULL`,
            `300`
        ],
        expected_output: `CAST("2020-01-01 01:04:59 UTC" AS TIMESTAMP)`
    },
    {
        inputs: [
            `CAST("2020-01-01 01:05:00 UTC" AS TIMESTAMP)`,
            `CAST("2020-01-01 01:04:59 UTC" AS TIMESTAMP)`,
            `300`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `CAST("2020-01-01 01:24:01 UTC" AS TIMESTAMP)`,
            `CAST("2020-01-01 01:09:01 UTC" AS TIMESTAMP)`,
            `300`
        ],
        expected_output: `CAST("2020-01-01 01:24:01 UTC" AS TIMESTAMP)`
    },
]);
generate_udf_test("ts_slide", [
    {
        inputs: [
            `CAST("2020-01-01 01:04:59 UTC" AS TIMESTAMP)`,
            `300`,
            `600`
        ],
        expected_output: `([STRUCT(CAST("2020-01-01 00:55:00 UTC" AS TIMESTAMP) AS window_start, CAST("2020-01-01 01:05:00 UTC" AS TIMESTAMP) AS window_end), STRUCT(CAST("2020-01-01 01:00:00 UTC" AS TIMESTAMP) AS window_start, CAST("2020-01-01 01:10:00 UTC" AS TIMESTAMP) AS window_end)])`
    },
    {
        inputs: [
            `CAST("2020-01-01 01:04:59 UTC" AS TIMESTAMP)`,
            `600`,
            `900`
        ],
        expected_output: `([STRUCT(CAST("2020-01-01 00:50:00 UTC" AS TIMESTAMP) AS window_start, CAST("2020-01-01 01:05:00 UTC" AS TIMESTAMP) AS window_end), STRUCT(CAST("2020-01-01 01:00:00 UTC" AS TIMESTAMP) AS window_start, CAST("2020-01-01 01:15:00 UTC" AS TIMESTAMP) AS window_end)])`
    },
    {
        inputs: [
            `CAST("2020-01-01 01:05:00 UTC" AS TIMESTAMP)`,
            `300`,
            `600`
        ],
        expected_output: `([STRUCT(CAST("2020-01-01 01:00:00 UTC" AS TIMESTAMP) AS window_start, CAST("2020-01-01 01:10:00 UTC" AS TIMESTAMP) AS window_end), STRUCT(CAST("2020-01-01 01:05:00 UTC" AS TIMESTAMP) AS window_start, CAST("2020-01-01 01:15:00 UTC" AS TIMESTAMP) AS window_end)])`
    },
    {
        inputs: [
            `CAST("2020-01-01 01:05:00 UTC" AS TIMESTAMP)`,
            `600`,
            `900`
        ],
        expected_output: `([STRUCT(CAST("2020-01-01 01:00:00 UTC" AS TIMESTAMP) AS window_start, CAST("2020-01-01 01:15:00 UTC" AS TIMESTAMP) AS window_end)])`
    },
]);
generate_udf_test("nlp_compromise_number", [
    {
        inputs: [`CAST("one hundred fifty seven" AS STRING)`],
        expected_output: `CAST(157 AS NUMERIC)`
    },
    {
        inputs: [`CAST("three point 5" AS STRING)`],
        expected_output: `CAST(3.5 AS NUMERIC)`
    },
    {
        inputs: [`CAST("2 hundred" AS STRING)`],
        expected_output: `CAST(200 AS NUMERIC)`
    },
    {
        inputs: [`CAST("minus 8" AS STRING)`],
        expected_output: `CAST(-8 AS NUMERIC)`
    },
    {
        inputs: [`CAST("5 million 3 hundred 25 point zero 1" AS STRING)`],
        expected_output: `CAST(5000325.01 AS NUMERIC)`
    },
]);
generate_udf_test("nlp_compromise_people", [
    // Below tests taken from https://github.com/spencermountain/compromise/blob/master/tests/people.test.js,
    {
        inputs: [`CAST("Mary is in the boat. Nancy is in the boat. Fred is in the boat. Jack is too." AS STRING)`],
        expected_output: `CAST(["mary", "nancy", "fred", "jack"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`CAST("jean jacket. jean Slkje" AS STRING)`],
        expected_output: `CAST(["jean slkje"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`CAST("The Bill was passed by James MacCarthur" AS STRING)`],
        expected_output: `CAST(["james maccarthur"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`CAST("Rod MacDonald bought a Rod" AS STRING)`],
        expected_output: `CAST(["rod macdonald"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`CAST("Matt 'the doctor' Smith lasted three seasons." AS STRING)`],
        expected_output: `CAST(["matt the doctor smith"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`CAST("Randal Kieth Orton and Dwayne 'the rock' Johnson had a really funny fight." AS STRING)`],
        expected_output: `CAST(["randal kieth orton", "dwayne the rock johnson"] AS ARRAY<STRING>)`
    },
]);
generate_udf_test("levenshtein", [
    {
        inputs: [
            `CAST("analyze" AS STRING)`,
            `CAST("analyse" AS STRING)`
        ],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [
            `CAST("opossum" AS STRING)`,
            `CAST("possum" AS STRING)`
        ],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [
            `CAST("potatoe" AS STRING)`,
            `CAST("potatoe" AS STRING)`
        ],
        expected_output: `CAST(0 AS INT64)`
    },
    {
        inputs: [
            `CAST("while" AS STRING)`,
            `CAST("whilst" AS STRING)`
        ],
        expected_output: `CAST(2 AS INT64)`
    },
    {
        inputs: [
            `CAST("aluminum" AS STRING)`,
            `CAST("alumininium" AS STRING)`
        ],
        expected_output: `CAST(3 AS INT64)`
    },
    {
        inputs: [
            `CAST("Connecticut" AS STRING)`,
            `CAST("CT" AS STRING)`
        ],
        expected_output: `CAST(10 AS INT64)`
    },
]);
generate_udf_test("getbit", [
    {
        inputs: [
            `CAST(23 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [
            `CAST(23 AS INT64)`,
            `CAST(3 AS INT64)`
        ],
        expected_output: `CAST(0 AS INT64)`
    }
]);
generate_udf_test("to_binary", [
    {
        inputs: [`CAST(123456 AS INT64)`],
        expected_output: `CAST("0000000000000000000000000000000000000000000000011110001001000000" AS STRING)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    }
]);
generate_udf_test("from_binary", [
    {
        inputs: [`CAST("0000000000000000000000000000000000000000000000011110001001000000" AS STRING)`],
        expected_output: `CAST(123456 AS INT64)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    }
]);
generate_udf_test("to_hex", [
    {
        inputs: [`CAST(123456 AS INT64)`],
        expected_output: `CAST("000000000001e240" AS STRING)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    }
]);
generate_udf_test("from_hex", [
    {
        inputs: [`CAST("000000000001e240" AS STRING)`],
        expected_output: `CAST(123456 AS INT64)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    }
]);
generate_udf_test("week_of_month", [
    {
        inputs: [`CAST("2020-07-03" AS DATE)`],
        expected_output: `CAST(0 AS INT64)`
    },
    {
        inputs: [`CAST("2020-07-08" AS DATE)`],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [`CAST("2020-07-10" AS DATE)`],
        expected_output: `CAST(1 AS INT64)`
    }
]);
generate_udf_test("day_occurrence_of_month", [
    {
        inputs: [`CAST("2020-07-03" AS DATE)`],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [`CAST("2020-07-08" AS DATE)`],
        expected_output: `CAST(2 AS INT64)`
    },
    {
        inputs: [`CAST("2020-07-10" AS DATE)`],
        expected_output: `CAST(2 AS INT64)`
    }
]);
//
//  Below targets StatsLib work
//
generate_udf_test("pvalue", [
    {
        inputs: [
            `CAST(0.3 AS FLOAT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST(0.8607079764250578 AS FLOAT64)`
    },
]);
generate_udf_test("kruskal_wallis", [
    {
        inputs: [`(SELECT [("a",1.0), ("b",2.0), ("c",2.3), ("a",1.4), ("b",2.2), ("c",5.5), ("a",1.0), ("b",2.3), ("c",2.3), ("a",1.1), ("b",7.2), ("c",2.8)])`],
        expected_output: `STRUCT(CAST(3.423076923076927 AS FLOAT64) AS H, CAST( 0.1805877514841956 AS FLOAT64) AS p, CAST(2 AS INT64) AS DoF)`
    },
]);
generate_udf_test("linear_regression", [
    {
        inputs: [`(SELECT [ (5.1,2.5), (5.0,2.0), (5.7,2.6), (6.0,2.2), (5.8,2.6), (5.5,2.3), (6.1,2.8), (5.5,2.5), (6.4,3.2), (5.6,3.0)])`],
        expected_output: `STRUCT(CAST(-0.4353361094588436 AS FLOAT64) AS a, CAST( 0.5300416418798544 AS FLOAT64) AS b, CAST(0.632366563565354 AS FLOAT64) AS r)`
    },
]);