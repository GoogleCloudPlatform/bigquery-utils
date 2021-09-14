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
        inputs: [`"STUDENT"`],
        expected_output: `"student"`
    },
    {
        inputs: [`"Foo"`],
        expected_output: `"foo"`
    },
    {
        inputs: [`"ÉTUDIANT"`],
        expected_output: `"Étudiant"`
    },
    {
        inputs: [`"ETUDIANT"`],
        expected_output: `"etudiant"`
    },
    {
        inputs: [`"aBCdef GH"`],
        expected_output: `"abcdef gh"`
    },
    {
        inputs: [`""`],
        expected_output: `""`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
generate_udf_test("substrb", [
    {
        inputs: [
            `"soupçon"`,
            `CAST(5 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `"ç"`
    },
    {
        inputs: [
            `"foobar"`,
            `CAST(1 AS INT64)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `"fo"`
    },
    {
        inputs: [
            `"foobar"`,
            `CAST(10 AS INT64)`,
            `CAST(2  AS INT64)`
        ],
        expected_output: `""`
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
            `""`,
            `CAST(3 AS INT64)`,
            `CAST(4 AS INT64)`
        ],
        expected_output: `""`
    },
]);
generate_udf_test("upperb", [
    {
        inputs: [`"étudiant"`],
        expected_output: `"éTUDIANT"`
    },
    {
        inputs: [`"etudiant"`],
        expected_output: `"ETUDIANT"`
    },
    {
        inputs: [`"foo"`],
        expected_output: `"FOO"`
    },
    {
        inputs: [`"aBCdef Gh"`],
        expected_output: `"ABCDEF GH"`
    },
    {
        inputs: [`""`],
        expected_output: `""`
    },
    {
        inputs: [`NULL`],
        expected_output: `NULL`
    },
]);
