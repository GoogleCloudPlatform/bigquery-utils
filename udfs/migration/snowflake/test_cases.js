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

const {generate_udf_test, generate_udaf_test} = unit_test_utils;

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

generate_udf_test("json_ilike", [
    {
        inputs: [`json_object('abc', 1, 'def', 2)`, `'_b_'`],
        expected_output: `JSON '{"abc": 1}'`
    },
    {
        inputs: [`json_object('abc', 1, 'cba', 2)`, `'%c%'`],
        expected_output: `JSON '{"abc": 1, "cba": 2}'`
    },
    {
        inputs: [`json_object('abc', 1, 'cba', 2)`, `'c%'`],
        expected_output: `JSON '{"cba": 2}'`
    }
]);

generate_udaf_test("object_agg",
    {
        input_columns: [`key`, `value`, `false NOT AGGREGATE`],
        input_rows: `
            select 'a' as key, JSON '1' as value
            union all
            select 'b' as key, JSON '2' as value
        `,
        expected_output: `JSON '{"a": 1, "b": 2}'`
    }
);

generate_udaf_test("object_agg",
    {
        input_columns: [`key`, `value`, `true NOT AGGREGATE`],
        input_rows: `
            select 'a' as key, JSON '1' as value
            union all
            select null as key, JSON '3' as value
            union all
            select 'b' as key, JSON '2' as value
            union all
            select 'b' as key, null as value
        `,
        expected_output: `JSON '{"a": 1, "b": 2}'`
    }
);

generate_udaf_test("object_agg",
    {
        input_columns: [`key`, `value`, `true NOT AGGREGATE`],
        input_rows: `
            select 'a' as key, JSON '1' as value
            union all
            select 'a' as key, JSON '1' as value
        `,
        expected_output: `JSON '{"a": 1}'`
    }
);

generate_udf_test("array_equal", [
    {
        inputs: [`[JSON '1', JSON '2']`, `[JSON '1', JSON '2']`],
        expected_output: `true`
    },
    {
        inputs: [`[JSON '1', JSON '2']`, `[JSON '1', JSON '3']`],
        expected_output: `false`
    },
    {
        inputs: [`[JSON '1', JSON '2']`, `[JSON '1']`],
        expected_output: `false`
    },
    {
        inputs: [`[JSON '1', CAST(NULL AS JSON)]`, `[JSON '1', CAST(NULL AS JSON)]`],
        expected_output: `true`
    },
    {
        inputs: [`[JSON '1']`, `[JSON '"1"']`],
        expected_output: `false`
    },
    {
        inputs: [`[JSON '{"a": 1}']`, `[JSON '{"a": 1}']`],
        expected_output: `true`
    },
    {
        inputs: [`[JSON '{"a": 1}']`, `[JSON '{"b": 1}']`],
        expected_output: `false`
    },
    {
        inputs: [`[JSON '{"a": 1}']`, `[JSON '{"a": 2}']`],
        expected_output: `false`
    },
    {
        inputs: [`[JSON '{"a": {"b": 1}}']`, `[JSON '{"a": {"b": 1}}']`],
        expected_output: `true`
    },
    {
        inputs: [`[JSON '{"a": {"b": 1}}']`, `[JSON '{"a": {"b": 2}}']`],
        expected_output: `false`
    },
    {
        inputs: [`[JSON '[1, 2]']`, `[JSON '[1, 2]']`],
        expected_output: `true`
    },
    {
        inputs: [`[JSON '[1, 2]']`, `[JSON '[1, 3]']`],
        expected_output: `false`
    },
    {
        inputs: [`CAST(NULL AS ARRAY<JSON>)`, `CAST(NULL AS ARRAY<JSON>)`],
        expected_output: `CAST(NULL AS BOOLEAN)`
    },
    {
        inputs: [`[JSON '1']`, `CAST(NULL AS ARRAY<JSON>)`],
        expected_output: `CAST(NULL AS BOOLEAN)`
    }
]);

generate_udf_test("bitmap_bucket_number", [
    {
        inputs: [`CAST(1 AS INT64)`],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [`CAST(1000000 AS INT64)`],
        expected_output: `CAST(31 AS INT64)`
    },
    {
        inputs: [`CAST(32767 AS INT64)`],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [`CAST(32768 AS INT64)`],
        expected_output: `CAST(1 AS INT64)`
    },
    {
        inputs: [`CAST(32769 AS INT64)`],
        expected_output: `CAST(2 AS INT64)`
    },
    {
        inputs: [`CAST(400000 AS INT64)`],
        expected_output: `CAST(13 AS INT64)`
    },
    {
        inputs: [`CAST(0 AS INT64)`],
        expected_output: `CAST(0 AS INT64)`
    }
]);

generate_udf_test("bitmap_bit_position", [
    {
        inputs: [`CAST(1 AS INT64)`],
        expected_output: `CAST(0 AS INT64)`
    },
    {
        inputs: [`CAST(1000000 AS INT64)`],
        expected_output: `CAST(16959 AS INT64)`
    },
    {
        inputs: [`CAST(32767 AS INT64)`],
        expected_output: `CAST(32766 AS INT64)`
    },
    {
        inputs: [`CAST(32768 AS INT64)`],
        expected_output: `CAST(32767 AS INT64)`
    },
    {
        inputs: [`CAST(32769 AS INT64)`],
        expected_output: `CAST(0 AS INT64)`
    },
    {
        inputs: [`CAST(400000 AS INT64)`],
        expected_output: `CAST(6783 AS INT64)`
    },
    {
        inputs: [`CAST(0 AS INT64)`],
        expected_output: `CAST(0 AS INT64)`
    }
]);
