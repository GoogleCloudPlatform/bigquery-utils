// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 "CAST(the "License");
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

generate_udf_test("factorial", [
    {
        inputs: [`CAST(0 AS INT64)`],
        expected_output: `CAST(1.0 AS NUMERIC)`
    },
    {
        inputs: [`CAST(1 AS INT64)`],
        expected_output: `CAST(1.0 AS NUMERIC)`
    },
    {
        inputs: [`CAST(5 AS INT64)`],
        expected_output: `CAST(120.0 AS NUMERIC)`
    },
    {
        inputs: [`CAST(10 AS INT64)`],
        expected_output: `CAST(3628800.0 AS NUMERIC)`
    }
]);

generate_udf_test("flatten", [
    {
        inputs: [`json_object('a', 1)`, `''`, `false`, `false`, `'both'`],
        expected_output: `[STRUCT(1 AS SEQ, "a" AS KEY, "a" AS PATH, CAST(NULL AS INT64) AS INDEX, JSON "1" AS VALUE, json_object('a', 1) AS THIS)]`
    },
    {
        inputs: [`json_array(1, 77)`, `''`, `false`, `false`, `'both'`],
        expected_output: `[STRUCT(1 AS SEQ, CAST(null AS STRING) AS KEY, "[0]" AS PATH, 0 AS INDEX, JSON "1" AS VALUE, json_array(1,77) AS THIS),` +
                          `STRUCT(2 AS SEQ, CAST(null AS STRING) AS KEY, "[1]" AS PATH, 1 AS INDEX, JSON "77" AS VALUE, json_array(1,77) AS THIS)]`
    },
    {
        inputs: [`json_object('a', 1, 'b', json_array(77, 88))`, `''`, `false`, `false`, `'both'`],
        expected_output: `[STRUCT(1 AS SEQ, "a" AS KEY, "a" AS PATH, null AS INDEX, JSON "1" AS VALUE, json_object('a', 1, 'b', json_array(77, 88)) AS THIS),` +
                          `STRUCT(2 AS SEQ, "b" AS KEY, "b" AS PATH, null AS INDEX, JSON_ARRAY(77,88) AS VALUE, json_object('a', 1, 'b', json_array(77, 88)) AS THIS)]`
    },
    {
        inputs: [`json_array()`, `''`, `false`, `false`, `'both'`],
        expected_output: `CAST([] AS ARRAY<STRUCT<SEQ INT64, KEY STRING, PATH STRING, INDEX INT64, VALUE JSON, THIS JSON>>)`
    },
    {
        inputs: [`json_array()`, `''`, `true`, `false`, `'both'`],
        expected_output: `[STRUCT(1 AS SEQ, CAST(NULL AS STRING) AS KEY, CAST(NULL AS STRING) AS PATH, CAST(NULL AS INT64) AS INDEX, CAST(NULL AS JSON) AS VALUE, json_array() AS THIS)]`
    },
    {
        inputs: [`json_object('a', 1, 'b', json_array(77, 88))`, `'b'`, `false`, `false`, `'both'`],
        expected_output: `[STRUCT(1 AS SEQ, CAST(NULL AS STRING) AS KEY, "b[0]" AS PATH, 0 AS INDEX, JSON "77" AS VALUE, json_array(77,88) AS THIS),` +
                          `STRUCT(2 AS SEQ, CAST(NULL AS STRING) AS KEY, "b[1]" AS PATH, 1 AS INDEX, JSON "88" AS VALUE, json_array(77,88) AS THIS)]`
    },
    {
        inputs: [`json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X'))`, `''`, `true`, `false`, `'both'`],
        expected_output: `[STRUCT(1 AS SEQ, "a" AS KEY, "a" AS PATH, CAST(NULL AS INT64) AS INDEX, JSON '1' AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(2 AS SEQ, "b" AS KEY, "b" AS PATH, CAST(NULL AS INT64) AS INDEX, json_array(77, 88) AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(3 AS SEQ, "c" AS KEY, "c" AS PATH, CAST(NULL AS INT64) AS INDEX, json_object('d', 'X') AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS)]`
    },
    {
        inputs: [`json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X'))`, `''`, `true`, `true`, `'both'`],
        expected_output: `[STRUCT(1 AS SEQ, "a" AS KEY, "a" AS PATH, CAST(NULL AS INT64) AS INDEX, JSON '1' AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(2 AS SEQ, "b" AS KEY, "b" AS PATH, CAST(NULL AS INT64) AS INDEX, json_array(77, 88) AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(1 AS SEQ, CAST(NULL AS STRING) AS KEY, "b[0]" AS PATH, 0 AS INDEX, JSON "77" AS VALUE, json_array(77, 88) AS THIS),` +
                          `STRUCT(2 AS SEQ, CAST(NULL AS STRING) AS KEY, "b[1]" AS PATH, 1 AS INDEX, JSON "88" AS VALUE, json_array(77, 88) AS THIS),` +
                          `STRUCT(3 AS SEQ, "c" AS KEY, "c" AS PATH, CAST(NULL AS INT64) AS INDEX, json_object('d', 'X') AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(1 AS SEQ, "d" AS KEY, "c.d" AS PATH, CAST(NULL AS INT64) AS INDEX, JSON '"X"' AS VALUE, json_object('d', 'X') AS THIS)]`
    },
    {
        inputs: [`json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X'))`, `''`, `true`, `true`, `'object'`],
        expected_output: `[STRUCT(1 AS SEQ, "a" AS KEY, "a" AS PATH, CAST(NULL AS INT64) AS INDEX, JSON '1' AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(2 AS SEQ, "b" AS KEY, "b" AS PATH, CAST(NULL AS INT64) AS INDEX, json_array(77, 88) AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(3 AS SEQ, "c" AS KEY, "c" AS PATH, CAST(NULL AS INT64) AS INDEX, json_object('d', 'X') AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(1 AS SEQ, "d" AS KEY, "c.d" AS PATH, CAST(NULL AS INT64) AS INDEX, JSON '"X"' AS VALUE, json_object('d', 'X') AS THIS)]`
    },
    {
        inputs: [`json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X'))`, `''`, `true`, `true`, `'array'`],
        expected_output: `[STRUCT(1 AS SEQ, "a" AS KEY, "a" AS PATH, CAST(NULL AS INT64) AS INDEX, JSON '1' AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(2 AS SEQ, "b" AS KEY, "b" AS PATH, CAST(NULL AS INT64) AS INDEX, json_array(77, 88) AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS),` +
                          `STRUCT(1 AS SEQ, CAST(NULL AS STRING) AS KEY, "b[0]" AS PATH, 0 AS INDEX, JSON "77" AS VALUE, json_array(77, 88) AS THIS),` +
                          `STRUCT(2 AS SEQ, CAST(NULL AS STRING) AS KEY, "b[1]" AS PATH, 1 AS INDEX, JSON "88" AS VALUE, json_array(77, 88) AS THIS),` +
                          `STRUCT(3 AS SEQ, "c" AS KEY, "c" AS PATH, CAST(NULL AS INT64) AS INDEX, json_object('d', 'X') AS VALUE, json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')) AS THIS)]`
    },
]);

