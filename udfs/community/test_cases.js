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
        inputs: [`"-1"`],
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
generate_udf_test("json_extract_keys", [
    {
        inputs: [`'{"foo" : "cat", "bar": "dog", "hat": "rat"}'`],
        expected_output: `["foo", "bar", "hat"]`
    },
    {
        inputs: [`'{"int" : 1}'`],
        expected_output: `["int"]`
    },
    {
        inputs: [`'invalid_json'`],
        expected_output: `cast(null as array<string>)`,
    },
    {
        inputs: [`string(null)`],
        expected_output: `cast(null as array<string>)`,
    },
]);
generate_udf_test("json_extract_values", [
    {
        inputs: [`'{"foo" : "cat", "bar": "dog", "hat": "rat"}'`],
        expected_output: `["cat", "dog", "rat"]`
    },
    {
        inputs: [`'{"int" : 1}'`],
        expected_output: `["1"]`
    },
    {
        inputs: [`'invalid_json'`],
        expected_output: `cast(null as array<string>)`,
    },
    {
        inputs: [`string(null)`],
        expected_output: `cast(null as array<string>)`,
    },
]);
generate_udf_test("json_typeof", [
    {
        inputs: [`'""'`],
        expected_output: `"string"`
    },
    {
        inputs: [`'"test"'`],
        expected_output: `"string"`
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
    {
        inputs: [`'string containing " double-quote'`],
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
generate_udf_test("url_decode", [
    {
        inputs: [
            `"https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A"`,
            `NULL`
        ],
        expected_output: `"https://example.com/?id=あいうえお"`
    },
    {
        inputs: [
            `"https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A"`,
            `"decodeURIComponent"`
        ],
        expected_output: `"https://example.com/?id=あいうえお"`
    },
    {
        inputs: [
            `"https://example.com/?id=%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A"`,
            `"decodeURI"`
        ],
        expected_output: `"https://example.com/?id=あいうえお"`
    },
    {
        inputs: [
            `"https%3A//example.com/%3Fid%3D%u3042%u3044%u3046%u3048%u304A"`,
            `"unescape"`
        ],
        expected_output: `"https://example.com/?id=あいうえお"`
    },
    {
        inputs: [
            `CAST(NULL AS STRING)`,
            `CAST(NULL AS STRING)`
        ],
        expected_output: `CAST(NULL AS STRING)`
    },
]);
generate_udf_test("url_encode", [
    {
        inputs: [
            `"https://example.com/?id=あいうえお"`,
            `NULL`
        ],
        expected_output: `"https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A"`
    },
    {
        inputs: [
            `"https://example.com/?id=あいうえお"`,
            `"encodeURIComponent"`
        ],
        expected_output: `"https%3A%2F%2Fexample.com%2F%3Fid%3D%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A"`
    },
    {
        inputs: [
            `"https://example.com/?id=あいうえお"`,
            `"encodeURI"`
        ],
        expected_output: `"https://example.com/?id=%E3%81%82%E3%81%84%E3%81%86%E3%81%88%E3%81%8A"`
    },
    {
        inputs: [
            `"https://example.com/?id=あいうえお"`,
            `"escape"`
        ],
        expected_output: `"https%3A//example.com/%3Fid%3D%u3042%u3044%u3046%u3048%u304A"`
    },
    {
        inputs: [
            `CAST(NULL AS STRING)`,
            `CAST(NULL AS STRING)`
        ],
        expected_output: `CAST(NULL AS STRING)`
    },
]);
generate_udf_test("url_parse", [
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"HOST"`
        ],
        expected_output: `"facebook.com"`
    },
    {
        inputs: [
            `"rpc://facebook.com/"`,
            `"HOST"`
        ],
        expected_output: `"facebook.com"`
    },
    {
        inputs: [
            `"subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"HOST"`
        ],
        expected_output: `"subdomain.facebook.com"`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"PATH"`
        ],
        expected_output: `"path1/p.php"`
    },
    {
        inputs: [
            `"http://facebook.com/~tilde/hy-phen/do.t/under_score.php?k1=v1&k2=v2#Ref1"`,
            `"PATH"`
        ],
        expected_output: `"~tilde/hy-phen/do.t/under_score.php"`
    },
    {
        inputs: [
            `"subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"PATH"`
        ],
        expected_output: `"path1/p.php"`
    },
    {
        inputs: [
            `"rpc://facebook.com/"`,
            `"PATH"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"QUERY"`
        ],
        expected_output: `"k1=v1&k2=v2#Ref1"`
    },
    {
        inputs: [
            `"rpc://facebook.com/"`,
            `"QUERY"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"QUERY"`
        ],
        expected_output: `"k1=v1&k2=v2#Ref1"`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"REF"`
        ],
        expected_output: `"Ref1"`
    },
    {
        inputs: [
            `"rpc://facebook.com/"`,
            `"REF"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"REF"`
        ],
        expected_output: `"Ref1"`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"PROTOCOL"`
        ],
        expected_output: `"http"`
    },
    {
        inputs: [
            `"rpc://facebook.com/"`,
            `"PROTOCOL"`
        ],
        expected_output: `"rpc"`
    },
    {
        inputs: [
            `"subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"PROTOCOL"`
        ],
        expected_output: `NULL`
    },
]);
generate_udf_test("url_param", [
    {
        inputs: [
            `NULL`,
            `"k1"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"k1"`
        ],
        expected_output: `"v1"`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"k2"`
        ],
        expected_output: `"v2"`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1"`,
            `"k3"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1&k4=v4"`,
            `"k4"`
        ],
        expected_output: `NULL`
    },
]);
generate_udf_test("url_trim_query", [
    {
        inputs: [
            `NULL`,
            `["hello"]`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"https://www.example.com/abc/index.html"`,
            `[]`
        ],
        expected_output: `"https://www.example.com/abc/index.html"`
    },
    {
        inputs: [
            `"https://www.example.com/abc/index.html"`,
            `NULL`
        ],
        expected_output: `"https://www.example.com/abc/index.html"`
    },
    {
        inputs: [
          `"https://www.example.com/abc/index.html?id=12345&utm_id=abc123&hello=world"`,
          `["utm_id", "id"]`
        ],
        expected_output: `"https://www.example.com/abc/index.html?hello=world"`
    },
    {
        inputs: [
          `"https://www.example.com/abc/index.html?id=12345&utm_id=abc123"`,
          `["utm_id"]`
        ],
        expected_output: `"https://www.example.com/abc/index.html?id=12345"`
    },
    {
        inputs: [
          `"https://www.example.com/abc/index.html?id=12345&utm_id=abc123"`,
          `["utm_id", "id"]`
        ],
        expected_output: `"https://www.example.com/abc/index.html"`
    },
    {
        inputs: [
            `"https://www.example.com/abc/index.html?id=12345&utm_id=abc123#hash"`,
            `["utm_id"]`
        ],
        expected_output: `"https://www.example.com/abc/index.html?id=12345#hash"`
    },
    {
        inputs: [
            `"https://www.example.com/abc/index.html?id=12345&utm_id=abc123#hash"`,
            `["utm_id", "id"]`
        ],
        expected_output: `"https://www.example.com/abc/index.html#hash"`
    },
    {
        inputs: [
            `"https://www.example.com/abc/index.html"`,
            `["gclid"]`
        ],
        expected_output: `"https://www.example.com/abc/index.html"`
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
        expected_output: `([STRUCT("abc" AS series_key, CAST("2020-01-01 00:30:00" AS TIMESTAMP) AS tumble_val), STRUCT("abc" AS series_key, CAST("2020-01-01 00:31:00" AS TIMESTAMP) AS tumble_val)])`
    },
    {
        inputs: [
            `ARRAY<STRING>["abc", "def"]`,
            `CAST(60 AS INT64)`,
            `CAST("2020-01-01 00:30:00" AS TIMESTAMP)`,
            `CAST("2020-01-01 00:30:30" AS TIMESTAMP)`
        ],
        expected_output: `([STRUCT("abc" AS series_key, CAST("2020-01-01 00:30:00" AS TIMESTAMP) AS tumble_val), STRUCT("def" AS series_key, CAST("2020-01-01 00:30:00" AS TIMESTAMP) AS tumble_val)])`
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
        inputs: [`"one hundred fifty seven"`],
        expected_output: `CAST(157 AS NUMERIC)`
    },
    {
        inputs: [`"three point 5"`],
        expected_output: `CAST(3.5 AS NUMERIC)`
    },
    {
        inputs: [`"2 hundred"`],
        expected_output: `CAST(200 AS NUMERIC)`
    },
    {
        inputs: [`"minus 8"`],
        expected_output: `CAST(-8 AS NUMERIC)`
    },
    {
        inputs: [`"5 million 3 hundred 25 point zero 1"`],
        expected_output: `CAST(5000325.01 AS NUMERIC)`
    },
]);
generate_udf_test("nlp_compromise_people", [
    // Below tests taken from https://github.com/spencermountain/compromise/blob/master/tests/people.test.js,
    {
        inputs: [`"Mary is in the boat. Nancy is in the boat. Fred is in the boat. Jack is too."`],
        expected_output: `CAST(["mary", "nancy", "fred", "jack"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`"jean jacket. jean Slkje"`],
        expected_output: `CAST(["jean slkje"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`"The Bill was passed by James MacCarthur"`],
        expected_output: `CAST(["james maccarthur"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`"Rod MacDonald bought a Rod"`],
        expected_output: `CAST(["rod macdonald"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`"Matt 'the doctor' Smith lasted three seasons."`],
        expected_output: `CAST(["matt the doctor smith"] AS ARRAY<STRING>)`
    },
    {
        inputs: [`"Randal Kieth Orton and Dwayne 'the rock' Johnson had a really funny fight."`],
        expected_output: `CAST(["randal kieth orton", "dwayne the rock johnson"] AS ARRAY<STRING>)`
    },
]);
generate_udf_test("levenshtein", [
    {
        inputs: [
            `"analyze"`,
            `"analyse"`
        ],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [
            `"opossum"`,
            `"possum"`
        ],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [
            `"potatoe"`,
            `"potatoe"`
        ],
        expected_output: `CAST(0 AS INT64)`
    },
    {
        inputs: [
            `"while"`,
            `"whilst"`
        ],
        expected_output: `CAST(2 AS INT64)`
    },
    {
        inputs: [
            `"aluminum"`,
            `"alumininium"`
        ],
        expected_output: `CAST(3 AS INT64)`
    },
    {
        inputs: [
            `"Connecticut"`,
            `"CT"`
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
        expected_output: `"0000000000000000000000000000000000000000000000011110001001000000"`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    }
]);
generate_udf_test("from_binary", [
    {
        inputs: [`"0000000000000000000000000000000000000000000000011110001001000000"`],
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
        expected_output: `"000000000001e240"`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    }
]);
generate_udf_test("from_hex", [
    {
        inputs: [`"000000000001e240"`],
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
generate_udf_test("chisquare_cdf", [
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
generate_udf_test("corr_pvalue", [
    {
	inputs: [
                `CAST(0.9 AS FLOAT64)`,
                `CAST(25 AS INT64)`
	],
	expected_output: `CAST(1.443229117741041E-9 AS FLOAT64)`
    },
    {
        inputs: [
                `CAST(-0.5 AS FLOAT64)`,
                `CAST(40 AS INT64)`
        ],
        expected_output: `CAST(0.0010423414457657223 AS FLOAT64)`
    },
    {
        inputs: [
                `CAST(1.0 AS FLOAT64)`,
                `CAST(50 AS INT64)`
        ],
        expected_output: `CAST(0.0 AS FLOAT64)`
    },
]);
generate_udf_test("p_fisherexact", [
    {
        inputs: [
		`CAST(90 AS FLOAT64)`,
		`CAST(27 AS FLOAT64)`,
		`CAST(17 AS FLOAT64)`,
		`CAST(50 AS FLOAT64)`
	],
        expected_output: `CAST(8.046828829103659E-12 AS FLOAT64)`
    },
]);
generate_udf_test("t_test", [
    {
        inputs: [
		`(SELECT ARRAY<FLOAT64>[13.3,6.0,20.0,8.0,14.0,19.0,18.0,25.0,16.0,24.0,15.0,1.0,15.0])`,
		`(SELECT ARRAY<FLOAT64>[22.0,16.0,21.7,21.0,30.0,26.0,12.0,23.2,28.0,23.0])`
	],
	expected_output: `STRUCT(CAST(2.8957935572829476 AS FLOAT64) AS t_value, CAST(21 AS INTEGER) AS dof)`
    },
]);
generate_udf_test("mannwhitneyu", [
    {
        inputs: [
		`(SELECT ARRAY<FLOAT64>[2, 4, 6, 2, 3, 7, 5, 1.])`,
		`(SELECT ARRAY<FLOAT64>[8, 10, 11, 14, 20, 18, 19, 9.])`,
		`CAST('two-sided' AS STRING)`
	],
	expected_output: `STRUCT(CAST(64.0 AS FLOAT64) AS U, CAST(9.391056991171487E-4 AS FLOAT64) AS p)`
    },
]);
//
generate_udf_test("normal_cdf", [
    {
        inputs: [
		`CAST(1.1 AS FLOAT64)`,
		`CAST(1.7 AS FLOAT64)`,
		`CAST(2.0 AS FLOAT64)`
	],
	expected_output: `CAST(0.3820885778110474 AS FLOAT64)`
    },
]);
//
// End of StatsLib work tests
//
generate_udf_test("jaccard", [
    {
        inputs: [`"thanks"`, `"thaanks"`],
        expected_output: `CAST(1.0 AS FLOAT64)`
    },
    {
        inputs: [`"thanks"`, `"thanxs"`],
        expected_output: `CAST(0.71 AS FLOAT64)`
    },
    {
        inputs: [`"bad demo"`, `"abd demo"`],
        expected_output: `CAST(1.0 AS FLOAT64)`
    },
    {
        inputs: [`"edge case"`, `"no match"`],
        expected_output: `CAST(0.25 AS FLOAT64)`
    },
]);
generate_udf_test("knots_to_mph", [
    {
        inputs: [`CAST(37.7 AS FLOAT64)`],
        expected_output: `CAST(43.384406 AS FLOAT64)`
    },
    {
        inputs: [`CAST(0.0 AS FLOAT64)`],
        expected_output: `CAST(0.0 AS FLOAT64)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("mph_to_knots", [
    {
        inputs: [`CAST(75.5 AS FLOAT64)`],
        expected_output: `CAST(65.607674794487224 AS FLOAT64)`
    },
    {
        inputs: [`CAST(0.0 AS FLOAT64)`],
        expected_output: `CAST(0.0 AS FLOAT64)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("meters_to_miles", [
    {
        inputs: [`CAST(5000.0 AS FLOAT64)`],
        expected_output: `CAST(3.1068559611866697 AS FLOAT64)`
    },
    {
        inputs: [`CAST(0.0 AS FLOAT64)`],
        expected_output: `CAST(0.0 AS FLOAT64)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("miles_to_meters", [
    {
        inputs: [`CAST(2.73 AS FLOAT64)`],
        expected_output: `CAST(4393.50912 AS FLOAT64)`
    },
    {
        inputs: [`CAST(0.0 AS FLOAT64)`],
        expected_output: `CAST(0.0 AS FLOAT64)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("nautical_miles_conversion", [
    {
        inputs: [`CAST(1.12 AS FLOAT64)`],
        expected_output: `CAST(1.2888736 AS FLOAT64)`
    },
    {
        inputs: [`CAST(0.0 AS FLOAT64)`],
        expected_output: `CAST(0.0 AS FLOAT64)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("azimuth_to_geog_point", [
    {
        inputs: [`CAST(30.2672 AS FLOAT64)`,`CAST(97.7431 AS FLOAT64)`,`CAST(312.9 AS FLOAT64)`,`CAST(1066.6 AS FLOAT64)`],
        expected_output: `ST_GEOGPOINT(81.4417483906444, 39.9606210457152)`
    },
    {
        inputs: [`CAST(0.0 AS FLOAT64)`,`CAST(0.0 AS FLOAT64)`,`CAST(0.0 AS FLOAT64)`,`CAST(0.0 AS FLOAT64)`],
        expected_output: `ST_GEOGPOINT(0, 0)`
    },
    {
        inputs: [`CAST(30.2672 AS FLOAT64)`,`CAST(97.7431 AS FLOAT64)`,`CAST(312.9 AS FLOAT64)`,`NULL`],
        expected_output: `NULL`
    },
    {
        inputs: [`NULL`,`NULL`,`NULL`,`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("cw_instr4", [
    {
        inputs: [
            `"TestStr123456"`,
		        `"Str"`,
		        `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `CAST(5 AS INT64)`
    },
]);
generate_udf_test("cw_initcap", [
    {
        inputs: [
            `"teststr"`
        ],
        expected_output: `"Teststr"`
    },
    {
        inputs: [
            `"test str"`
        ],
        expected_output: `"Test Str"`
    },
]);
generate_udf_test("cw_otranslate", [
    {
        inputs: [
            `"Thin and Thick"`,
            `"Thk"`,
            `"Sp"`
        ],
        expected_output: `"Spin and Spic"`
    },
]);
generate_udf_test("cw_stringify_interval", [
    {
        inputs: [
            `CAST(86100 AS INT64)`
        ],
        expected_output: `"+0000 23:55:00"`
    },
    {
        inputs: [
            `CAST(86400 AS INT64)`
        ],
        expected_output: `"+0001 00:00:00"`
    },
]);
generate_udf_test("cw_regex_mode", [
    {
        inputs: [
            `"i"`
        ],
        expected_output: `"ig"`
    },
    {
        inputs: [
            `"m"`
        ],
        expected_output: `"mg"`
    },
    {
        inputs: [
            `"n"`
        ],
        expected_output: `"sg"`
    },
    {
        inputs: [
            `"a"`
        ],
        expected_output: `"g"`
    },
]);
generate_udf_test("cw_regexp_substr_4", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Test"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"Test"`
    },
    {
        inputs: [
            `"TestStr123456"`,
            `"123"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"123"`
    },
]);
generate_udf_test("cw_regexp_substr_generic", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Test"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `"g"`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `"Test"`
    },
    {
        inputs: [
            `"TestStr123456Test"`,
            `"test"`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`,
            `"ig"`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `"Test"`
    },
]);
generate_udf_test("cw_regexp_substr_5", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Test"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `"g"`
        ],
        expected_output: `"Test"`
    },
    {
        inputs: [
            `"TestStr123456Test"`,
            `"test"`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`,
            `"i"`
        ],
        expected_output: `"Test"`
    },
]);
generate_udf_test("cw_regexp_substr_6", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Test"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `"g"`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `"Test"`
    },
    {
        inputs: [
            `"TestStr123456Test"`,
            `"test"`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`,
            `"i"`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `"Test"`
    },
]);
generate_udf_test("cw_map_create", [
    {
        inputs: [
            `CAST([1, 2, 3] AS ARRAY<INT64>)`,
            `CAST(["A", "B", "C"] AS ARRAY<STRING>)`
        ],
        expected_output: `([STRUCT(CAST(1 AS INT64) AS key, "A" AS value), 
                           STRUCT(CAST(2 AS INT64) AS key, "B" AS value),
                           STRUCT(CAST(3 AS INT64) AS key, "C" AS value)])`
    },
]);
generate_udf_test("cw_map_get", [
    {
        inputs: [
            `([STRUCT(CAST(1 AS INT64) AS key, "ABC" AS value)])`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"ABC"`
    },
]);
generate_udf_test("cw_regexp_instr_2", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`
        ],
        expected_output: `CAST(5 AS INT64)`
    },
    {
        inputs: [
            `"TestStr123456"`,
            `"90"`
        ],
        expected_output: `CAST(0 AS INT64)`
    },
]);
generate_udf_test("cw_regexp_instr_3", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `CAST(5 AS INT64)`
    },
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `CAST(6 AS INT64)`
        ],
        expected_output: `CAST(0 AS INT64)`
    },
]);
generate_udf_test("cw_regexp_instr_4", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `CAST(5 AS INT64)`
    },
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST(0 AS INT64)`
    },
]);
generate_udf_test("cw_regexp_instr_generic", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `"i"`
        ],
        expected_output: `CAST(8 AS INT64)`
    },
]);
generate_udf_test("cw_regexp_instr_6", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `"i"`
        ],
        expected_output: `CAST(8 AS INT64)`
    },
]);
generate_udf_test("cw_regexp_replace_generic", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `"Gbp"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `"i"`
        ],
        expected_output: `"TestGbp123456"`
    },
]);
generate_udf_test("cw_regexp_replace_4", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `"Cad$"`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"TestCad$123456"`
    },
]);
generate_udf_test("cw_regexp_replace_5", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `"Usd$#"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"TestUsd$#123456"`
    },
    {
        inputs: [
            `"TestStr123456Str"`,
            `"Str"`,
            `"Usd$#"`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `"TestStr123456Usd$#"`
    },
    {
        inputs: [
            `"TestStr123456Str"`,
            `"Str"`,
            `"Usd$#"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"TestUsd$#123456Str"`
    },
]);
generate_udf_test("cw_regexp_replace_6", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `"$:#>"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `"i"`
        ],
        expected_output: `"Test$:#>123456"`
    },
]);
generate_udf_test("cw_regexp_instr_5", [
    {
        inputs: [
            `"TestStr123456"`,
            `"123"`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `CAST(11 AS INT64)`
    },
]);
generate_udf_test("cw_array_min", [
    {
        inputs: [
            `(SELECT ARRAY<INT64>[1, 2, 3, 4, 5, 6])`
        ],
        expected_output: `CAST(1 AS INT64)`
    },
]);
generate_udf_test("cw_array_median", [
    {
        inputs: [
            `(SELECT ARRAY<INT64>[1, 2, 3, 4, 5, 6])`
        ],
        expected_output: `CAST(3.5 AS FLOAT64)`
    },
]);
generate_udf_test("cw_array_max", [
    {
        inputs: [
            `(SELECT ARRAY<INT64>[1, 2, 3, 4, 5, 6])`
        ],
        expected_output: `CAST(6 AS INT64)`
    },
]);
generate_udf_test("cw_array_distinct", [
    {
        inputs: [
            `(SELECT ARRAY<INT64>[1, 2, 3, 4, 4, 5, 5])`
        ],
        expected_output: `CAST([1, 2, 3, 4, 5] AS ARRAY<INT64>)`
    },
]);
generate_udf_test("cw_next_day", [
    {
        inputs: [
            `CAST('2022-09-21' AS DATE)`,
            `"we"`
        ],
        expected_output: `CAST('2022-09-28' AS DATE)`
    },
]);
generate_udf_test("cw_td_nvp", [
    {
        inputs: [
            `"entree:orange chicken#entree2:honey salmon"`,
            `"entree"`,
            `"#"`,
            `":"`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"orange chicken"`
    },
]);
generate_udf_test("cw_convert_base", [
    {
        inputs: [
            `"001101011"`,
            `CAST(2 AS INT64)`,
            `CAST(10 AS INT64)`
        ],
        expected_output: `"107"`
    },
    {
        inputs: [
            `"A"`,
            `CAST(16 AS INT64)`,
            `CAST(8 AS INT64)`
        ],
        expected_output: `"12"`
    },
    {
        inputs: [
            `"17"`,
            `CAST(16 AS INT64)`,
            `CAST(10 AS INT64)`
        ],
        expected_output: `"23"`
    },
])
generate_udf_test("cw_from_base", [
    {
        inputs: [
            `"001101011"`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST(107 AS INT64)`
    },
    {
        inputs: [
            `"37"`,
            `CAST(8 AS INT64)`
        ],
        expected_output: `CAST(31 AS INT64)`
    },
    {
        inputs: [
            `"A"`,
            `CAST(16 AS INT64)`
        ],
        expected_output: `CAST(10 AS INT64)`
    },
]);
generate_udf_test("cw_to_base", [
    {
        inputs: [
            `CAST(5 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `"101"`
    },
    {
        inputs: [
            `CAST(31 AS INT64)`,
            `CAST(8 AS INT64)`
        ],
        expected_output: `"37"`
    },
    {
        inputs: [
            `CAST(10 AS INT64)`,
            `CAST(16 AS INT64)`
        ],
        expected_output: `"a"`
    },
]);
generate_udf_test("cw_array_overlap", [
    {
        inputs: [
            `(SELECT ARRAY<INT64>[1, 2, 3])`,
            `(SELECT ARRAY<INT64>[4, 5, 6])`
        ],
        expected_output: `CAST(false AS BOOL)`
    },
    {
        inputs: [
            `(SELECT ARRAY<INT64>[1, 2, 3])`,
            `(SELECT ARRAY<INT64>[2, 3, 4])`
        ],
        expected_output: `CAST(true AS BOOL)`
    },
]);
generate_udf_test("cw_array_compact", [
    {
        inputs: [
            `(SELECT ARRAY<INT64>[1, 2, 3])`
        ],
        expected_output: `CAST([1, 2, 3] AS ARRAY<INT64>)`
    },
]);
generate_udf_test("cw_runtime_parse_interval_seconds", [
    {
        inputs: [
            `"1 day"`
        ],
        expected_output: `CAST(86400 AS INT64)`
    },
    {
        inputs: [
            `"1 DAY"`
        ],
        expected_output: `CAST(86400 AS INT64)`
    },
]);
generate_udf_test("cw_url_encode", [
    {
        inputs: [
            `"?"`
        ],
        expected_output: `"%3F"`
    },
    {
        inputs: [
            `"/"`
        ],
        expected_output: `"%2F"`
    },
]);
generate_udf_test("cw_url_decode", [
    {
        inputs: [
            `"%3F"`
        ],
        expected_output: `"?"`
    },
    {
        inputs: [
            `"%2F"`
        ],
        expected_output: `"/"`
    },
]);
generate_udf_test("cw_url_extract_host", [
    {
        inputs: [
            `"https://google.com"`
        ],
        expected_output: `"google.com"`
    },
    {
        inputs: [
            `"   www.Example.Co.UK    "`
        ],
        expected_output: `"www.Example.Co.UK"`
    },
]);
generate_udf_test("cw_url_extract_protocol", [
    {
        inputs: [
            `"https://google.com/test?key=val"`
        ],
        expected_output: `"https"`
    },
]);
generate_udf_test("cw_url_extract_path", [
    {
        inputs: [
            `"https://www.test.com/collections-in-java#collectionmethods"`
        ],
        expected_output: `"/collections-in-java"`
    },
]);
generate_udf_test("cw_url_extract_port", [
    {
        inputs: [
            `"https://localhost:8080/test?key=val"`
        ],
        expected_output: `CAST(8080 AS INT64)`
    },
]);
generate_udf_test("cw_url_extract_authority", [
    {
        inputs: [
            `"https://localhost:8080/test?key=val"`
        ],
        expected_output: `"localhost:8080"`
    },
]);
generate_udf_test("cw_url_extract_query", [
    {
        inputs: [
            `"https://localhost:8080/test?key=val"`
        ],
        expected_output: `"key=val"`
    },
]);
generate_udf_test("cw_url_extract_file", [
    {
        inputs: [
            `"https://www.test.com/collections-in-java#collectionmethods"`
        ],
        expected_output: `"/collections-in-java"`
    },
]);
generate_udf_test("cw_url_extract_fragment", [
    {
        inputs: [
            `"https://www.test.com/collections-in-java#collectionmethods"`
        ],
        expected_output: `"collectionmethods"`
    },
]);
generate_udf_test("cw_url_extract_parameter", [
    {
        inputs: [
            `"https://www.test.com/collections-in-java&key=val#collectionmethods"`,
            `"key"`
        ],
        expected_output: `"val"`
    },
]);
generate_udf_test("cw_regexp_extract", [
    {
        inputs: [
            `"TestStr123456#?%&"`,
            `"Str"`
        ],
        expected_output: `"Str"`
    },
]);
generate_udf_test("cw_regexp_extract_n", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str"`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `"Str"`
    },
]);
generate_udf_test("cw_regexp_extract_all", [
    {
        inputs: [
            `"TestStr123456"`,
            `"Str.*"`
        ],
        expected_output: `CAST(["Str123456"] AS ARRAY<STRING>)`
    },
]);
generate_udf_test("cw_regexp_extract_all_n", [
    {
        inputs: [
            `"TestStr123456Str789"`,
            `"Str.*"`,
            `CAST(0 AS INT64)`
        ],
        expected_output: `CAST(["Str123456Str789"] AS ARRAY<STRING>)`
    },
]);
generate_udf_test("cw_json_array_contains_str", [
    {
        inputs: [
            `'["name", "test", "valid"]'`,
            `"test"`
        ],
        expected_output: `CAST(true AS BOOL)`
    },
]);
generate_udf_test("cw_json_array_contains_num", [
    {
        inputs: [
            `'[1, 2, 3, "valid"]'`,
            `CAST(1.0 AS FLOAT64)`
        ],
        expected_output: `CAST(true AS BOOL)`
    },
    {
        inputs: [
            `'[1, 2, 3, "valid"]'`,
            `CAST(5.0 AS FLOAT64)`
        ],
        expected_output: `CAST(false AS BOOL)`
    },
]);
generate_udf_test("cw_json_array_contains_bool", [
    {
        inputs: [
            `'[1, 2, 3, "valid", true]'`,
            `CAST(true AS BOOL)`
        ],
        expected_output: `CAST(true AS BOOL)`
    },
    {
        inputs: [
            `'[1, 2, 3, "valid", true]'`,
            `CAST(false AS BOOL)`
        ],
        expected_output: `CAST(false AS BOOL)`
    },
]);
generate_udf_test("cw_json_array_get", [
    {
        inputs: [
            `'[{"name": "test"}, {"name": "test1"}]'`,
            `CAST(1.0 AS FLOAT64)`
        ],
        expected_output: `'{"name":"test1"}'`
    },
]);
generate_udf_test("cw_json_array_length", [
    {
        inputs: [
            `'[{"name": "test"}, {"name": "test1"}]'`
        ],
        expected_output: `CAST(2 AS INT64)`
    },
]);
generate_udf_test("cw_substring_index", [
    {
        inputs: [
            `"TestStr123456,Test123"`,
            `","`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"TestStr123456"`
    },
]);
generate_udf_test("cw_editdistance", [
    {
        inputs: [
            `"Jim D. Swain"`,
            `"John Smith"`
        ],
        expected_output: `CAST(9 AS INT64)`
    },
    {
        inputs: [
            `"Jim D. Swain"`,
            `"Jim D. Swain"`
        ],
        expected_output: `CAST(0 AS INT64)`
    },
]);
generate_udf_test("cw_round_half_even", [
    {
        inputs: [
            `CAST(10 AS BIGNUMERIC)`,
            `CAST(10 AS INT64)`
        ],
        expected_output: `CAST(10 AS NUMERIC)`
    },
]);
generate_udf_test("cw_round_half_even_bignumeric", [
    {
        inputs: [
            `CAST(10 AS BIGNUMERIC)`,
            `CAST(10 AS INT64)`
        ],
        expected_output: `CAST(10 AS BIGNUMERIC)`
    },
]);
generate_udf_test("cw_getbit", [
    {
        inputs: [
            `CAST(11 AS INT64)`,
            `CAST(100 AS INT64)`
        ],
        expected_output: `CAST(0 AS INT64)`
    },
    {
        inputs: [
            `CAST(11 AS INT64)`,
            `CAST(3 AS INT64)`
        ],
        expected_output: `CAST(1 AS INT64)`
    },
]);
generate_udf_test("cw_setbit", [
    {
        inputs: [
            `CAST(1001 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST(1005 AS INT64)`
    },
]);
generate_udf_test("cw_lower_case_ascii_only", [
    {
        inputs: [
            `"TestStr123456#"`
        ],
        expected_output: `"teststr123456#"`
    },
]);
generate_udf_test("cw_substrb", [
    {
        inputs: [
            `"TestStr123"`,
            `CAST(0 AS INT64)`,
            `CAST(3 AS INT64)`
        ],
        expected_output: `"Te"`
    },
]);
generate_udf_test("cw_twograms", [
    {
        inputs: [
            `"Test Str 123456 789"`
        ],
        expected_output: `CAST(["Test Str", "Str 123456", "123456 789"] AS ARRAY<STRING>)`
    },
]);
generate_udf_test("cw_threegrams", [
    {
        inputs: [
            `"Test 1234 str abc"`
        ],
        expected_output: `CAST(["Test 1234 str", "1234 str abc"] AS ARRAY<STRING>)`
    },
]);
generate_udf_test("cw_nvp2json1", [
    {
        inputs: [
            `"name=google&occupation=engineer&hair=color"`
        ],
        expected_output: `'{"name":"google","occupation":"engineer","hair":"color"}'`
    },
]);
generate_udf_test("cw_nvp2json3", [
    {
        inputs: [
            `"name=google&occupation=engineer&hair=color"`,
            `"&"`,
            `"="`
        ],
        expected_output: `'{"name":"google","occupation":"engineer","hair":"color"}'`
    },
]);
generate_udf_test("cw_nvp2json4", [
    {
        inputs: [
            `"name=google#1&occupation=engineer#2&hair=color#3"`,
            `"&"`,
            `"="`,
            `"#"`
        ],
        expected_output: `'{"name":"google1","occupation":"engineer2","hair":"color3"}'`
    },
]);
generate_udf_test("cw_strtok", [
    {
        inputs: [
            `"Test#1"`,
            `"#"`
        ],
        expected_output: `([STRUCT(CAST(1 AS INT64) AS tokennumber, "Test" AS token), 
                           STRUCT(CAST(2 AS INT64) AS tokennumber, "1" AS token)])`
    },
]);
generate_udf_test("cw_regexp_split", [
    {
        inputs: [
            `"Test#1"`,
            `"#"`,
            `"i"`
        ],
        expected_output: `([STRUCT(CAST(1 AS INT64) AS tokennumber, "Test" AS token), 
                           STRUCT(CAST(2 AS INT64) AS tokennumber, "1" AS token)])`
    },
]);
generate_udf_test("cw_csvld", [
    {
        inputs: [
            `"Test#123"`,
            `"#"`,
            `'"'`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST(["Test", "123"] AS ARRAY<STRING>)`
    },
]);
generate_udf_test("cw_json_enumerate_array", [
    {
        inputs: [
            `'[{"name":"Cameron"}, {"name":"John"}]'`
        ],
        expected_output: `([STRUCT(CAST(1 AS INT64) AS ordinal, '{"name":"Cameron"}' AS jsonvalue), 
                           STRUCT(CAST(2 AS INT64) AS ordinal, '{"name":"John"}' AS jsonvalue)])`
    },
]);
generate_udf_test("cw_ts_pattern_match", [
    {
        inputs: [
            `CAST(["abc", "abc"] AS ARRAY<STRING>)`,
            `CAST(["abc"] AS ARRAY<STRING>)`
        ],
        expected_output: `([STRUCT(CAST(1 AS INT64) AS pattern_id, CAST(1 AS INT64) AS start, CAST(1 AS INT64) AS stop),
                            STRUCT(CAST(2 AS INT64) AS pattern_id, CAST(2 AS INT64) AS start, CAST(2 AS INT64) AS stop)])`
    },
]);
generate_udf_test("cw_error_number", [
    {
        inputs: [
            `"Error Message"`,
        ],
        expected_output: `CAST(1 AS INT64)`
    },
]);
generate_udf_test("cw_error_severity", [
    {
        inputs: [
            `"Error Message"`,
        ],
        expected_output: `CAST(1 AS INT64)`
    },
]);
generate_udf_test("cw_error_state", [
    {
        inputs: [
            `"Error Message"`,
        ],
        expected_output: `CAST(1 AS INT64)`
    },
]);
generate_udf_test("cw_find_in_list", [
    {
        inputs: [
            `"1"`,
            `"[Test,1,2]"`
        ],
        expected_output: `CAST(2 AS INT64)`
    },
]);
generate_udf_test("cw_map_parse", [
    {
        inputs: [
            `"a=1 b=42"`,
            `" "`,
            `"="`
        ],
        expected_output: `([STRUCT("a" AS key, "1" AS value), 
                           STRUCT("b" AS key, "42" AS value)])`
    },
]);
generate_udf_test("cw_comparable_format_varchar_t", [
    {
        inputs: [
            `"2"`
        ],
        expected_output: `"32"`
    },
]);
generate_udf_test("cw_comparable_format_varchar", [
    {
        inputs: [
            `ARRAY<STRING>["2", "8"]`
        ],
        expected_output: `"32 38"`
    },
]);
generate_udf_test("cw_comparable_format_bigint_t", [
    {
        inputs: [
            `CAST(2 AS INT64)`
        ],
        expected_output: `"p                  2"`
    },
]);
generate_udf_test("cw_comparable_format_bigint", [
    {
        inputs: [
            `ARRAY<INT64>[2, 8]`
        ],
        expected_output: `"p                  2 p                  8"`
    },
]);
generate_udf_test("cw_ts_overlap_buckets", [
    {
        inputs: [
            `CAST(false AS BOOL)`,
            `([STRUCT(TIMESTAMP("2008-12-25"), TIMESTAMP("2008-12-31")), 
            STRUCT(TIMESTAMP("2008-12-26"), TIMESTAMP("2008-12-30"))])`
        ],
        expected_output: `([STRUCT(1 AS bucketNo, CAST("2008-12-25 00:00:00 UTC" AS TIMESTAMP) AS st, CAST("2008-12-31 00:00:00 UTC" AS TIMESTAMP) AS et)])`
    },
]);
generate_udf_test("cw_months_between", [
    {
        inputs: [
            `DATETIME "2022-02-01 00:00:00"`,
            `DATETIME "2022-01-31 00:00:00"`
        ],
        expected_output: `CAST("0.03225806451612903225806451612903225806" AS BIGNUMERIC)`
    },
    {
        inputs: [
            `DATETIME "2022-03-01 11:00:00"`,
            `DATETIME "2022-02-01 10:00:00"`
        ],
        expected_output: `CAST("1" AS BIGNUMERIC)`
    },
    {
        inputs: [
            `DATETIME "2022-03-01 11:34:56"`,
            `DATETIME "2022-02-28 10:22:33"`
        ],
        expected_output: `CAST("0.13064516129032258064516129032258064516" AS BIGNUMERIC)`
    }
]);
generate_udf_test("interval_seconds", [
  {
    inputs: [
      `INTERVAL -1 DAY`,
    ],
    expected_output: `CAST(-86400 AS INT64)`
  },
]);

generate_udf_test("interval_millis", [
  {
    inputs: [
      `INTERVAL -1 DAY`,
    ],
    expected_output: `CAST(-86400000 AS INT64)`
  },
]);

generate_udf_test("interval_micros", [
  {
    inputs: [
      `INTERVAL -1 DAY`,
    ],
    expected_output: `CAST(-86400000000 AS INT64)`
  },
]);

generate_udf_test("bignumber_add", [
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"2348592348793428978934278932746531725371625376152367153761536715376"`
        ],
        expected_output: `"102348592348793428978934278932746531725371625376152367153761536715375"`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `""`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `NULL`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("bignumber_div", [
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"33333333333333333333333333333333333333333333333333333333333333333333"`
        ],
        expected_output: `"3"`
    },
    {
        inputs: [
            `"0"`,
            `"33333333333333333333333333333333333333333333333333333333333333333333"`
        ],
        expected_output: `"0"`
    },
    {
        inputs: [
            `NULL`,
            `"33333333333333333333333333333333333333333333333333333333333333333333"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `""`,
            `"33333333333333333333333333333333333333333333333333333333333333333333"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"0"`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `NULL`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `""`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("bignumber_mul", [
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"893427328732842662772591830391462182598436547786876876876"`
        ],
        expected_output: `"89342732873284266277259183039146218259843654778687687687599999999999106572671267157337227408169608537817401563452213123123124"`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `""`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `NULL`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `NULL`,
            `NULL`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("bignumber_sub", [
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"893427328732842662772591830391462182598436547786876876876"`
        ],
        expected_output: `"99999999999106572671267157337227408169608537817401563452213123123123"`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `""`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `NULL`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("bignumber_sum", [
    {
        inputs: [
            `ARRAY<STRING>[
                "99999999999999999999999999999999999999999999999999999999999999999999", 
                "893427328732842662772591830391462182598436547786876876876",
                "123456789123456789123456789123456789123456789123456789123456789123456789"
            ]`
        ],
        expected_output: `"123556789123457682550785521966119561715287180585639387560004576000333664"`
    },
    {
        inputs: [
            `ARRAY<STRING>[
                "99999999999999999999999999999999999999999999999999999999999999999999", 
                "",
                "123456789123456789123456789123456789123456789123456789123456789123456789"
            ]`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `ARRAY<STRING>[
                "99999999999999999999999999999999999999999999999999999999999999999999", 
                "893427328732842662772591830391462182598436547786876876876",
                NULL
            ]`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("bignumber_avg", [
    {
        inputs: [
            `ARRAY<STRING>[
                "99999999999999999999999999999999999999999999999999999999999999999999", 
                "33333333333333333333333333333333333333333333333333333333333333333333",
                "66666666666666666666666666666666666666666666666666666666666666666666"
            ]`
        ],
        expected_output: `"66666666666666666666666666666666666666666666666666666666666666666666"`
    },
    {
        inputs: [
            `ARRAY<STRING>[
                "99999999999999999999999999999999999999999999999999999999999999999999", 
                "",
                "123456789123456789123456789123456789123456789123456789123456789123456789"
            ]`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `ARRAY<STRING>[
                "99999999999999999999999999999999999999999999999999999999999999999999", 
                "893427328732842662772591830391462182598436547786876876876",
                NULL
            ]`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `ARRAY<STRING>[]`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("bignumber_gt", [
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"99999999999999999999999999999999999999999999999999999999999999999998"`
        ],
        expected_output: `CAST(true AS BOOL)`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"99999999999999999999999999999999999999999999999999999999999999999999"`
        ],
        expected_output: `CAST(false AS BOOL)`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"100000000000000000000000000000000000000000000000000000000000000000000"`
        ],
        expected_output: `CAST(false AS BOOL)`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `"-99999999999999999999999999999999999999999999999999999999999999999999"`
        ],
        expected_output: `CAST(true AS BOOL)`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `""`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `"99999999999999999999999999999999999999999999999999999999999999999999"`,
            `NULL`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("cw_period_intersection", [
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`
        ],
        expected_output: `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-22 00:00:00' AS lower, TIMESTAMP '2001-11-26 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-23 00:00:00' AS lower, TIMESTAMP '2001-11-25 00:00:00' AS upper)`
        ],
        expected_output: `STRUCT(TIMESTAMP '2001-11-23 00:00:00' AS lower, TIMESTAMP '2001-11-25 00:00:00' AS upper)`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-10 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`
        ],
        expected_output: `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-14 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-15 00:00:00' AS lower, TIMESTAMP '2001-11-16 00:00:00' AS upper)`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("cw_period_ldiff", [
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`
        ],
        expected_output: `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-13 00:00:00' AS upper)`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-22 00:00:00' AS lower, TIMESTAMP '2001-11-26 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-23 00:00:00' AS lower, TIMESTAMP '2001-11-25 00:00:00' AS upper)`
        ],
        expected_output: `STRUCT(TIMESTAMP '2001-11-22 00:00:00' AS lower, TIMESTAMP '2001-11-23 00:00:00' AS upper)`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-15 00:00:00' AS lower, TIMESTAMP '2001-11-16 00:00:00' AS upper)`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-14 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("cw_period_rdiff", [
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`
        ],
        expected_output: `STRUCT(TIMESTAMP '2001-11-14 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-22 00:00:00' AS lower, TIMESTAMP '2001-11-26 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-23 00:00:00' AS lower, TIMESTAMP '2001-11-25 00:00:00' AS upper)`
        ],
        expected_output: `STRUCT(TIMESTAMP '2001-11-25 00:00:00' AS lower, TIMESTAMP '2001-11-26 00:00:00' AS upper)`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-15 00:00:00' AS lower, TIMESTAMP '2001-11-16 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-13 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `STRUCT(TIMESTAMP '2001-11-14 00:00:00' AS lower, TIMESTAMP '2001-11-15 00:00:00' AS upper)`,
            `STRUCT(TIMESTAMP '2001-11-12 00:00:00' AS lower, TIMESTAMP '2001-11-14 00:00:00' AS upper)`
        ],
        expected_output: `NULL`
    },
]);

generate_udf_test("sure_nonnull", [
  {
    inputs: [
      `"string_example"`,
    ],
    expected_output: `"string_example"`
  }
]);

generate_udf_test("sure_nonnull", [
  {
    inputs: [
      `1`,
    ],
    expected_output: `1`
  }
]);

generate_udf_test("sure_cond", [
  {
    inputs: [
      `1`,
      `TRUE`,
    ],
    expected_output: `1`
  },
]);

generate_udf_test("sure_like", [
  {
    inputs: [
      `"[Testcase]"`,
      `"[%]"`,
    ],
    expected_output: `"[Testcase]"`
  },
]);

generate_udf_test("sure_range", [
  {
    inputs: [
      `1`,
      `1`,
      `10`,
    ],
    expected_output: `1`,
  }
]);

generate_udf_test("sure_range", [
  {
    inputs: [
      `"b"`,
      `"a"`,
      `"c"`,
    ],
    expected_output: `"b"`,
  }
]);

generate_udf_test("sure_values", [
  {
    inputs: [
      `"hoge"`,
      `["hoge"]`
    ],
    expected_output: `"hoge"`
  },
  {
    inputs: [
      `STRING(null)`,
      `["hoge"]`
    ],
    expected_output: `NULL`
  }
]);
