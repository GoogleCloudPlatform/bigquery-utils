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

const { generate_udf_test } = unit_test_utils;

generate_udf_test("initcap", [
    {
        input_1: "CAST('hello world' AS STRING)",
        expected_output: "CAST('Hello World' AS STRING)"
    },
    {
        input_1: "CAST('ALL CAPS' AS STRING)",
        expected_output: "CAST('All Caps' AS STRING)"
    },
    {
        input_1: `CAST("does comma's behavior affect anything?" AS STRING)`,
        expected_output: `CAST("Does Comma'S Behavior Affect Anything?" AS STRING)`
    },
    {
        input_1: "CAST('All non-musical theatre' AS STRING)",
        expected_output: "CAST('All Non-Musical Theatre' AS STRING)"
    },
    {
        input_1: "CAST('All symphony, concerto, and choir concerts' AS STRING)",
        expected_output: "CAST('All Symphony, Concerto, And Choir Concerts' AS STRING)"
    },
    {
        input_1: 'CAST("À vaillant coeur rien d’impossible" AS STRING)',
        expected_output: 'CAST("À Vaillant Coeur Rien D’Impossible" AS STRING)'
    },
    {
        input_1: "CAST('640 k!ouGht tO BE enough~for_anyONE' AS STRING)",
        expected_output: "CAST('640 K!Ought To Be Enough~For_Anyone' AS STRING)"
    },
    {
        input_1: "CAST('Simplicity & élÉgance are unpopular because they require hard-work&discipline' AS STRING)",
        expected_output: "CAST('Simplicity & Élégance Are Unpopular Because They Require Hard-Work&Discipline' AS STRING)"
    },
    {
        input_1: `CAST("one+one is   '(two-one)*[two]'" AS STRING)`,
        expected_output: `CAST("One+One Is   '(Two-One)*[Two]'" AS STRING)`
    },
    {
        input_1: "CAST('<lorem>ipsum@GMAIL.COM' AS STRING)",
        expected_output: "CAST('<Lorem>Ipsum@Gmail.Com' AS STRING)"
    }
]);

generate_udf_test("translate", [
    {
        input_1: "CAST('Etiam.laoreet.libero@sodalesMaurisblandit.edu' AS STRING)",
        input_2: "CAST('@' AS STRING)",
        input_3: "CAST('.' AS STRING)",
        expected_output: "CAST('Etiam.laoreet.libero.sodalesMaurisblandit.edu' AS STRING)"
    }
]);

generate_udf_test("split_part", [
    {
        input_1: 'CAST("2020-02-01" AS STRING)',
        input_2: 'CAST("-" AS STRING)',
        input_3: 'CAST(1 AS INT64)',
        expected_output: 'CAST("2020" AS STRING)'
    },
    {
        input_1: 'CAST("2020-02-01" AS STRING)',
        input_2: 'CAST("-" AS STRING)',
        input_3: 'CAST(2 AS INT64)',
        expected_output: 'CAST("02" AS STRING)'
    },
    {
        input_1: 'CAST("2020-02-01" AS STRING)',
        input_2: 'CAST("-" AS STRING)',
        input_3: 'CAST(3 AS INT64)',
        expected_output: 'CAST("01" AS STRING)'
    },
    {
        input_1: 'CAST("2020-02-01" AS STRING)',
        input_2: 'CAST("-" AS STRING)',
        input_3: 'CAST(5 AS INT64)',
        expected_output: 'CAST("" AS STRING)'
    }
]);