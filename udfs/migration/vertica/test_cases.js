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

generate_udf_test("lowerb", [
    {
        inputs: [`CAST("STUDENT" AS STRING)`],
        expected_output: `CAST("student" AS STRING)`
    },
    {
        inputs: [`CAST("Foo" AS STRING)`],
        expected_output: `CAST("foo" AS STRING)`
    },
    {
        inputs: [`CAST("ÉTUDIANT" AS STRING)`],
        expected_output: `CAST("Étudiant" AS STRING)`
    },
    {
        inputs: [`CAST("ETUDIANT" AS STRING)`],
        expected_output: `CAST("etudiant" AS STRING)`
    },
    {
        inputs: [`CAST("aBCdef GH" AS STRING)`],
        expected_output: `CAST("abcdef gh" AS STRING)`
    },
    {
        inputs: [`CAST("" AS STRING)`],
        expected_output: `CAST("" AS STRING)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("substrb", [
    {
        inputs: [
            `CAST("soupçon" AS STRING)`,
            `CAST(5 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST("ç" AS STRING)`
    },
    {
        inputs: [
            `CAST("foobar" AS STRING)`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST("fo" AS STRING)`
    },
    {
        inputs: [
            `CAST("foobar" AS STRING)`,
            `CAST(10 AS INT64)`,
            `CAST(2  AS INT64)`
        ],
        expected_output: `CAST("" AS STRING)`
    },
    {
        inputs: [
            `NULL`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `NULL`
    },
    {
        inputs: [
            `CAST("" AS STRING)`,
            `CAST(3 AS INT64)`,
            `CAST(4 AS INT64)`
        ],
        expected_output: `CAST("" AS STRING)`
    },
]);
generate_udf_test("upperb", [
    {
        inputs: [`CAST("étudiant" AS STRING)`],
        expected_output: `CAST("éTUDIANT" AS STRING)`
    },
    {
        inputs: [`CAST("etudiant" AS STRING)`],
        expected_output: `CAST("ETUDIANT" AS STRING)`
    },
    {
        inputs: [`CAST("foo" AS STRING)`],
        expected_output: `CAST("FOO" AS STRING)`
    },
    {
        inputs: [`CAST("aBCdef Gh" AS STRING)`],
        expected_output: `CAST("ABCDEF GH" AS STRING)`
    },
    {
        inputs: [`CAST("" AS STRING)`],
        expected_output: `CAST("" AS STRING)`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
