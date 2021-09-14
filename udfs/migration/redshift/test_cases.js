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
        inputs: [`'hello world'`],
        expected_output: `'Hello World'`
    },
    {
        inputs: [`'ALL CAPS'`],
        expected_output: `'All Caps'`
    },
    {
        inputs: [`"does comma's behavior affect anything?"`],
        expected_output: `"Does Comma'S Behavior Affect Anything?"`
    },
    {
        inputs: [`'All non-musical theatre'`],
        expected_output: `'All Non-Musical Theatre'`
    },
    {
        inputs: [`'All symphony, concerto, and choir concerts'`],
        expected_output: `'All Symphony, Concerto, And Choir Concerts'`
    },
    {
        inputs: [`"À vaillant coeur rien d’impossible"`],
        expected_output: `"À Vaillant Coeur Rien D’Impossible"`
    },
    {
        inputs: [`'640 k!ouGht tO BE enough~for_anyONE'`],
        expected_output: `'640 K!Ought To Be Enough~For_Anyone'`
    },
    {
        inputs: [`'Simplicity & élÉgance are unpopular because they require hard-work&discipline'`],
        expected_output: `'Simplicity & Élégance Are Unpopular Because They Require Hard-Work&Discipline'`
    },
    {
        inputs: [`"one+one is   '(two-one)*[two]'"`],
        expected_output: `"One+One Is   '(Two-One)*[Two]'"`
    },
    {
        inputs: [`'<lorem>ipsum@GMAIL.COM'`],
        expected_output: `'<Lorem>Ipsum@Gmail.Com'`
    }
]);
generate_udf_test("translate", [
    {
        inputs: [
            `'Etiam.laoreet.libero@sodalesMaurisblandit.edu'`,
            `'@'`,
            `'.'`
        ],
        expected_output: `'Etiam.laoreet.libero.sodalesMaurisblandit.edu'`
    }
]);
generate_udf_test("split_part", [
    {
        inputs: [
            `"2020-02-01"`,
            `"-"`,
            `CAST(1 AS INT64)`
        ],
        expected_output: `"2020"`
    },
    {
        inputs: [
            `"2020-02-01"`,
            `"-"`,
            `CAST(2 AS INT64)`
        ],
        expected_output: `"02"`
    },
    {
        inputs: [
            `"2020-02-01"`,
            `"-"`,
            `CAST(3 AS INT64)`
        ],
        expected_output: `"01"`
    },
    {
        inputs: [
            `"2020-02-01"`,
            `"-"`,
            `CAST(5 AS INT64)`
        ],
        expected_output: `""`
    }
]);
