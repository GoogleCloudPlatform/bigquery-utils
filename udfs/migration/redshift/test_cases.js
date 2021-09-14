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

generate_udf_test("initcap", [
    {
        inputs: [`CAST('hello world' AS STRING)`],
        expected_output: `CAST('Hello World' AS STRING)`
    },
    {
        inputs: [`CAST('ALL CAPS' AS STRING)`],
        expected_output: `CAST('All Caps' AS STRING)`
    },
    {
        inputs: [`CAST("does comma's behavior affect anything?" AS STRING)`],
        expected_output: `CAST("Does Comma'S Behavior Affect Anything?" AS STRING)`
    },
    {
        inputs: [`CAST('All non-musical theatre' AS STRING)`],
        expected_output: `CAST('All Non-Musical Theatre' AS STRING)`
    },
    {
        inputs: [`CAST('All symphony, concerto, and choir concerts' AS STRING)`],
        expected_output: `CAST('All Symphony, Concerto, And Choir Concerts' AS STRING)`
    },
    {
        inputs: [`CAST("À vaillant coeur rien d’impossible" AS STRING)`],
        expected_output: `CAST("À Vaillant Coeur Rien D’Impossible" AS STRING)`
    },
    {
        inputs: [`CAST('640 k!ouGht tO BE enough~for_anyONE' AS STRING)`],
        expected_output: `CAST('640 K!Ought To Be Enough~For_Anyone' AS STRING)`
    },
    {
        inputs: [`CAST('Simplicity & élÉgance are unpopular because they require hard-work&discipline' AS STRING)`],
        expected_output: `CAST('Simplicity & Élégance Are Unpopular Because They Require Hard-Work&Discipline' AS STRING)`
    },
    {
        inputs: [`CAST("one+one is   '(two-one)*[two]'" AS STRING)`],
        expected_output: `CAST("One+One Is   '(Two-One)*[Two]'" AS STRING)`
    },
    {
        inputs: [`CAST('<lorem>ipsum@GMAIL.COM' AS STRING)`],
        expected_output: `CAST('<Lorem>Ipsum@Gmail.Com' AS STRING)`
    }
]);
generate_udf_test("translate", [
    {
        inputs: [
            `CAST('Etiam.laoreet.libero@sodalesMaurisblandit.edu' AS STRING)`,
            `CAST('@' AS STRING)`,
            `CAST('.' AS STRING)`
        ],
        expected_output: `CAST('Etiam.laoreet.libero.sodalesMaurisblandit.edu' AS STRING)`
    }
]);
generate_udf_test("split_part", [
    {
        inputs: [
            `CAST("2020-02-01" AS STRING)`,
            `CAST("-" AS STRING)`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `CAST("2020" AS STRING)`
    },
    {
        inputs: [
            `CAST("2020-02-01" AS STRING)`,
            `CAST("-" AS STRING)`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `CAST("02" AS STRING)`
    },
    {
        inputs: [
            `CAST("2020-02-01" AS STRING)`,
            `CAST("-" AS STRING)`,
            `CAST(3 AS INT64)`
        ],
        expected_output: `CAST("01" AS STRING)`
    },
    {
        inputs: [
            `CAST("2020-02-01" AS STRING)`,
            `CAST("-" AS STRING)`,
            `CAST(5 AS INT64)`
        ],
        expected_output: `CAST("" AS STRING)`
    }
]);
