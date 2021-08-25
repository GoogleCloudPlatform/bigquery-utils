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

generate_udf_test("lowerb", [
  {
    input_1: "CAST('STUDENT' AS STRING)",
    expected_output: "CAST('student' AS STRING)",
  },
  {
    input_1: "CAST('Foo' AS STRING)",
    expected_output: "CAST('foo' AS STRING)",
  },
  {
    input_1: "CAST('ÉTUDIANT' AS STRING)",
    expected_output: "CAST('Étudiant' AS STRING)",
  },
  {
    input_1: "CAST('ETUDIANT' AS STRING)",
    expected_output: "CAST('etudiant' AS STRING)",
  },
  {
    input_1: "CAST('aBCdef GH' AS STRING)",
    expected_output: "CAST('abcdef gh' AS STRING)",
  },
  {
    input_1: "CAST('' AS STRING)",
    expected_output: "CAST('' AS STRING)",
  },
  {
    input_1: "(NULL)",
    expected_output: "(NULL)",
  },
]);

generate_udf_test("substrb", [
  {
    input_1: "CAST('soupçon' AS STRING)",
    input_2: "CAST(5 AS INT64)",
    input_3: "CAST(2 AS INT64)",
    expected_output: "CAST('ç' AS STRING)",
  },
  {
    input_1: "CAST('foobar' AS STRING)",
    input_2: "CAST(1 AS INT64)",
    input_3: "CAST(2 AS INT64)",
    expected_output: "CAST('fo' AS STRING)",
  },
  {
    input_1: "CAST('foobar' AS STRING)",
    input_2: "CAST(10 AS INT64)",
    input_3: "CAST(2  AS INT64)",
    expected_output: "CAST('' AS STRING)",
  },
  {
    input_1: "(NULL)",
    input_2: "CAST(1 AS INT64)",
    input_3: "CAST(2 AS INT64)",
    expected_output: "(NULL)",
  },
  {
    input_1: "CAST('' AS STRING)",
    input_2: "CAST(3 AS INT64)",
    input_3: "CAST(4 AS INT64)",
    expected_output: "CAST('' AS STRING)",
  },
]);

generate_udf_test("upperb", [
  {
    input_1: "CAST('étudiant' AS STRING)",
    expected_output: "CAST('éTUDIANT' AS STRING)",
  },
  {
    input_1: "CAST('etudiant' AS STRING)",
    expected_output: "CAST('ETUDIANT' AS STRING)",
  },
  {
    input_1: "CAST('foo' AS STRING)",
    expected_output: "CAST('FOO' AS STRING)",
  },
  {
    input_1: "CAST('aBCdef Gh' AS STRING)",
    expected_output: "CAST('ABCDEF GH' AS STRING)",
  },
  {
    input_1: "CAST('' AS STRING)",
    expected_output: "CAST('' AS STRING)",
  },
  {
    input_1: "(NULL)",
    expected_output: "(NULL)",
  },
]);
