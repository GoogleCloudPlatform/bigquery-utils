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
    input_1: "CAST(-1 AS STRING)",
    expected_output: "CAST(-1 AS INT64)",
  },
]);
generate_udf_test("int", [
  {
    input_1: "CAST(1 AS INT64)",
    expected_output: "CAST(1 AS INT64)",
  },
  {
    input_1: "CAST(7 AS INT64)",
    expected_output: "CAST(7 AS INT64)",
  },
]);
generate_udf_test("int", [
  {
    input_1: "CAST(2.5 AS FLOAT64)",
    expected_output: "CAST(2 AS INT64)",
  },
  {
    input_1: "CAST(7.8 AS FLOAT64)",
    expected_output: "CAST(7 AS INT64)",
  },
]);
generate_udf_test("json_typeof", [
  {
    input_1: "('\"\"')",
    expected_output: "('string')",
  },
  {
    input_1: "('\"test\"')",
    expected_output: "('string')",
  },
  {
    input_1: "('true')",
    expected_output: "('boolean')",
  },
  {
    input_1: "('false')",
    expected_output: "('boolean')",
  },
  {
    input_1: "('null')",
    expected_output: "('null')",
  },
  {
    input_1: "('0')",
    expected_output: "('number')",
  },
  {
    input_1: "('1')",
    expected_output: "('number')",
  },
  {
    input_1: "('-1')",
    expected_output: "('number')",
  },
  {
    input_1: "('0.0')",
    expected_output: "('number')",
  },
  {
    input_1: "('1.0')",
    expected_output: "('number')",
  },
  {
    input_1: "('-1.0')",
    expected_output: "('number')",
  },
  {
    input_1: "('1e1')",
    expected_output: "('number')",
  },
  {
    input_1: "('-1e1')",
    expected_output: "('number')",
  },
  {
    input_1: "('[]')",
    expected_output: "('array')",
  },
  {
    input_1: "('[1, 2, 3]')",
    expected_output: "('array')",
  },
  {
    input_1: "('{}')",
    expected_output: "('object')",
  },
  {
    input_1: '  (\'{"foo":"bar"}\')',
    expected_output: "('object')",
  },
  {
    input_1: "('')",
    expected_output: "(NULL)",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "(b'test')",
    expected_output: "('BYTES')",
  },
  {
    input_1: "(b'')",
    expected_output: "('BYTES')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "('test')",
    expected_output: "('STRING')",
  },
  {
    input_1: "('')",
    expected_output: "('STRING')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "(TRUE)",
    expected_output: "('BOOL')",
  },
  {
    input_1: "(FALSE)",
    expected_output: "('BOOL')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "(NULL)",
    expected_output: "('NULL')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "(1)",
    expected_output: "('INT64')",
  },
  {
    input_1: "(-1)",
    expected_output: "('INT64')",
  },
  {
    input_1: "(0)",
    expected_output: "('INT64')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "(0.0)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "(-1.0)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "(1.0)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "(+123e45)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "(-123e45)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "(12e345)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "(-12e345)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "CAST('inf' AS FLOAT64)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "CAST('-inf' AS FLOAT64)",
    expected_output: "('FLOAT64')",
  },
  {
    input_1: "CAST('nan' AS FLOAT64)",
    expected_output: "('FLOAT64')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "DATE '1970-01-01'",
    expected_output: "('DATE')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "DATETIME '1970-01-01T00:00:00'",
    expected_output: "('DATETIME')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "TIME '00:00:00'",
    expected_output: "('TIME')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "TIMESTAMP '1970-01-01T00:00:00Z'",
    expected_output: "('TIMESTAMP')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "STRUCT()",
    expected_output: "('STRUCT')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "STRUCT(1)",
    expected_output: "('STRUCT')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "STRUCT(1, 2, 3)",
    expected_output: "('STRUCT')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "STRUCT<a INT64, b INT64, c INT64>(1, 2, 3)",
    expected_output: "('STRUCT')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "([])",
    expected_output: "('ARRAY')",
  },
  {
    input_1: "([1, 2, 3])",
    expected_output: "('ARRAY')",
  },
  {
    input_1: "ARRAY<INT64>[1, 2, 3]",
    expected_output: "('ARRAY')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "ST_GEOGPOINT(0, 0)",
    expected_output: "('GEOGRAPHY')",
  },
]);
generate_udf_test("typeof", [
  {
    input_1: "NUMERIC '0'",
    expected_output: "('NUMERIC')",
  },
  {
    input_1: "NUMERIC '1'",
    expected_output: "('NUMERIC')",
  },
  {
    input_1: "NUMERIC '-1'",
    expected_output: "('NUMERIC')",
  },
]);
generate_udf_test("url_parse", [
  {
    input_1: "CAST('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('HOST' AS STRING)",
    expected_output: "CAST('facebook.com' AS STRING)",
  },
  {
    input_1: "CAST('rpc://facebook.com/' AS STRING)",
    input_2: " CAST('HOST' AS STRING)",
    expected_output: "CAST('facebook.com' AS STRING)",
  },
  {
    input_1: "CAST('subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('HOST' AS STRING)",
    expected_output: "CAST('subdomain.facebook.com' AS STRING)",
  },
  {
    input_1: "CAST('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('PATH' AS STRING)",
    expected_output: "CAST('path1/p.php' AS STRING)",
  },
  {
    input_1: "CAST('subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('PATH' AS STRING)",
    expected_output: "CAST('path1/p.php' AS STRING)",
  },
  {
    input_1: "CAST('rpc://facebook.com/' AS STRING)",
    input_2: " CAST('PATH' AS STRING)",
    expected_output: "(NULL) # NULL is a type in YAML so wrap it in parenthesis",
  },
  {
    input_1: "CAST('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('QUERY' AS STRING)",
    expected_output: "CAST('k1=v1&k2=v2#Ref1' AS STRING)",
  },
  {
    input_1: "CAST('rpc://facebook.com/' AS STRING)",
    input_2: " CAST('QUERY' AS STRING)",
    expected_output: "(NULL) # NULL is a type in YAML so wrap it in parenthesis",
  },
  {
    input_1: "CAST('subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('QUERY' AS STRING)",
    expected_output: "CAST('k1=v1&k2=v2#Ref1' AS STRING)",
  },
  {
    input_1: "CAST('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('REF' AS STRING)",
    expected_output: "CAST('Ref1' AS STRING)",
  },
  {
    input_1: "CAST('rpc://facebook.com/' AS STRING)",
    input_2: " CAST('REF' AS STRING)",
    expected_output: "(NULL) # NULL is a type in YAML so wrap it in parenthesis",
  },
  {
    input_1: "CAST('subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('REF' AS STRING)",
    expected_output: "CAST('Ref1' AS STRING)",
  },
  {
    input_1: "CAST('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('PROTOCOL' AS STRING)",
    expected_output: "CAST('http' AS STRING)",
  },
  {
    input_1: "CAST('rpc://facebook.com/' AS STRING)",
    input_2: " CAST('PROTOCOL' AS STRING)",
    expected_output: "CAST('rpc' AS STRING)",
  },
  {
    input_1: "CAST('subdomain.facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
    input_2: " CAST('PROTOCOL' AS STRING)",
    expected_output: "(NULL) # NULL is a type in YAML so wrap it in parenthesis",
  },
]);
generate_udf_test("last_day", [
  {
    input_1: '  CAST("1987-12-25" AS DATE)',
    expected_output: 'CAST("1987-12-31" AS DATE)',
  },
  {
    input_1: 'CAST("1998-09-04" AS DATE)',
    expected_output: 'CAST("1998-09-30" AS DATE)',
  },
  {
    input_1: 'CAST("2020-02-21" AS DATE)',
    expected_output: 'CAST("2020-02-29" AS DATE) # leap year',
  },
  {
    input_1: 'CAST("2019-02-21" AS DATE)',
    expected_output: 'CAST("2019-02-28" AS DATE) # non-leap year',
  },
]);
generate_udf_test("percentage_change", [
  {
    input_1: "CAST(0.2 AS FLOAT64)",
    input_2: " CAST(0.4 AS FLOAT64)",
    expected_output: "CAST(1.0 AS FLOAT64)",
  },
  {
    input_1: "CAST(5 AS NUMERIC)",
    input_2: " CAST(15 AS NUMERIC)",
    expected_output: "CAST(2.0 AS FLOAT64)",
  },
  {
    input_1: "CAST(100 AS INT64)",
    input_2: " CAST(50 AS INT64)",
    expected_output: "CAST(-0.5 AS FLOAT64)",
  },
  {
    input_1: "CAST(-20 AS INT64)",
    input_2: " CAST(-45 AS INT64)",
    expected_output: "CAST(-1.25 AS FLOAT64)",
  },
  {
    input_1: "CAST(0 AS INT64)",
    input_2: " CAST(0 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(10 AS INT64)",
    input_2: " CAST(0 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(10 AS INT64)",
    input_2: " CAST(NULL AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(0 AS FLOAT64)",
    input_2: " CAST(10 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(NULL AS INT64)",
    input_2: " CAST(10 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
]);
generate_udf_test("percentage_difference", [
  {
    input_1: "CAST(0.22222222 AS FLOAT64)",
    input_2: " CAST(0.88888888 AS FLOAT64)",
    expected_output: "CAST(1.2 AS FLOAT64)",
  },
  {
    input_1: "CAST(0.2 AS NUMERIC)",
    input_2: " CAST(0.8 AS NUMERIC)",
    expected_output: "CAST(1.2 AS FLOAT64)",
  },
  {
    input_1: "CAST(2 AS INT64)",
    input_2: " CAST(8 AS INT64)",
    expected_output: "CAST(1.2 AS FLOAT64)",
  },
  {
    input_1: "CAST(100 AS INT64)",
    input_2: " CAST(200 AS INT64)",
    expected_output: "CAST(0.6667 AS FLOAT64)",
  },
  {
    input_1: "CAST(-2 AS INT64)",
    input_2: " CAST(8 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(2 AS INT64)",
    input_2: " CAST(-8 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(0 AS INT64)",
    input_2: " CAST(0 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(0 AS INT64)",
    input_2: " CAST(100 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(100 AS INT64)",
    input_2: " CAST(0 AS INT64)",
    expected_output: "CAST(NULL AS FLOAT64)",
  },
  {
    input_1: "CAST(1.0 AS FLOAT64)",
    input_2: " CAST(1000000000 AS INT64)",
    expected_output: "CAST(2.0 AS FLOAT64)",
  },
]);
generate_udf_test("linear_interpolate", [      {
    "input_1": "CAST(2 AS INT64)",
    "input_2": "STRUCT(CAST(1 AS INT64) AS x, CAST(1.0 AS FLOAT64) AS y)",
    "input_3": "STRUCT(CAST(3 AS INT64) AS x, CAST(3.0 AS FLOAT64) AS y)",
    "expected_output": "CAST(2.0 AS FLOAT64)",
    },
    {
    "input_1": "CAST(3 AS INT64)",
    "input_2": "STRUCT(CAST(1 AS INT64) AS x, CAST(1.0 AS FLOAT64) AS y)",
    "input_3": "STRUCT(CAST(4 AS INT64) AS x, CAST(4.0 AS FLOAT64) AS y)",
    "expected_output": "CAST(3.0 AS FLOAT64)",
    },
]);
generate_udf_test("ts_linear_interpolate", [      {
    "input_1": "CAST('2020-01-01 00:15:00' AS TIMESTAMP)",
    "input_2": "STRUCT(CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS x, CAST(1.0 AS FLOAT64))",
    "input_3": "STRUCT(CAST('2020-01-01 00:30:00' AS TIMESTAMP) AS x, CAST(3.0 AS FLOAT64))",
    "expected_output": "CAST(2.0 AS FLOAT64)",
    },
    {
    "input_1": "CAST('2020-01-01 00:15:00' AS TIMESTAMP)",
    "input_2": "STRUCT(CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS x, CAST(1.0 AS FLOAT64))",
    "input_3": "STRUCT(CAST('2020-01-01 02:30:00' AS TIMESTAMP) AS x, CAST(3.0 AS FLOAT64))",
    "expected_output": "CAST(1.2 AS FLOAT64)",
    },
]);
generate_udf_test("ts_tumble", [
  {
    input_1: "CAST('2020-01-01 00:17:30' AS TIMESTAMP)",
    input_2: " CAST(900 AS INT64)",
    expected_output: "CAST('2020-01-01 00:15:00' AS TIMESTAMP)",
  },
  {
    input_1: "CAST('2020-01-01 00:17:30' AS TIMESTAMP)",
    input_2: " CAST(600 AS INT64)",
    expected_output: "CAST('2020-01-01 00:10:00' AS TIMESTAMP)",
  },
  {
    input_1: "CAST('2020-01-01 00:17:30' AS TIMESTAMP)",
    input_2: " CAST(300 AS INT64)",
    expected_output: "CAST('2020-01-01 00:15:00' AS TIMESTAMP)",
  },
  {
    input_1: "CAST('2020-01-01 00:17:30' AS TIMESTAMP)",
    input_2: " CAST(60 AS INT64)",
    expected_output: "CAST('2020-01-01 00:17:00' AS TIMESTAMP)",
  },
  {
    input_1: "CAST('2020-01-01 00:17:30' AS TIMESTAMP)",
    input_2: " CAST(0 AS INT64)",
    expected_output: "(NULL)",
  },
]);
generate_udf_test("ts_gen_keyed_timestamps", [
  {
    input_1: "ARRAY<STRING>['abc']",
    input_2: " CAST(60 AS INT64)",
    input_3: " CAST('2020-01-01 00:30:00' AS TIMESTAMP)",
    input_4: " CAST('2020-01-01 00:31:00' AS TIMESTAMP)",
    expected_output: "([STRUCT(CAST('abc' AS STRING) AS series_key, CAST('2020-01-01 00:30:00' AS TIMESTAMP) AS tumble_val), STRUCT(CAST('abc' AS STRING) AS series_key, CAST('2020-01-01 00:31:00' AS TIMESTAMP) AS tumble_val)])",
  },
  {
    input_1: "ARRAY<STRING>['abc', 'def']",
    input_2: "CAST(60 AS INT64)",
    input_3: " CAST('2020-01-01 00:30:00' AS TIMESTAMP)",
    input_4: " CAST('2020-01-01 00:30:30' AS TIMESTAMP)",
    expected_output: "([STRUCT(CAST('abc' AS STRING) AS series_key, CAST('2020-01-01 00:30:00' AS TIMESTAMP) AS tumble_val), STRUCT(CAST('def' AS STRING) AS series_key, CAST('2020-01-01 00:30:00' AS TIMESTAMP) AS tumble_val)])",
  },
]);
generate_udf_test("ts_session_group", [
  {
    input_1: "CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP)",
    input_2: " (NULL)",
    input_3: " 300",
    expected_output: "CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP)",
  },
  {
    input_1: "CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP)",
    input_2: " CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP)",
    input_3: " 300",
    expected_output: "(NULL)",
  },
  {
    input_1: "CAST('2020-01-01 01:24:01 UTC' AS TIMESTAMP)",
    input_2: " CAST('2020-01-01 01:09:01 UTC' AS TIMESTAMP)",
    input_3: " 300",
    expected_output: "CAST('2020-01-01 01:24:01 UTC' AS TIMESTAMP)",
  },
]);
generate_udf_test("ts_slide", [
  {
    input_1: "CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP)",
    input_2: " 300",
    input_3: " 600",
    expected_output: "([STRUCT(CAST('2020-01-01 00:55:00 UTC' AS TIMESTAMP) AS window_start, CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP) AS window_end), STRUCT(CAST('2020-01-01 01:00:00 UTC' AS TIMESTAMP) AS window_start, CAST('2020-01-01 01:10:00 UTC' AS TIMESTAMP) AS window_end)])",
  },
  {
    input_1: "CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP)",
    input_2: " 600",
    input_3: " 900",
    expected_output: "([STRUCT(CAST('2020-01-01 00:50:00 UTC' AS TIMESTAMP) AS window_start, CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP) AS window_end), STRUCT(CAST('2020-01-01 01:00:00 UTC' AS TIMESTAMP) AS window_start, CAST('2020-01-01 01:15:00 UTC' AS TIMESTAMP) AS window_end)])",
  },
  {
    input_1: "CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP)",
    input_2: " 300",
    input_3: " 600",
    expected_output: "([STRUCT(CAST('2020-01-01 01:00:00 UTC' AS TIMESTAMP) AS window_start, CAST('2020-01-01 01:10:00 UTC' AS TIMESTAMP) AS window_end), STRUCT(CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP) AS window_start, CAST('2020-01-01 01:15:00 UTC' AS TIMESTAMP) AS window_end)])",
  },
  {
    input_1: "CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP)",
    input_2: " 600",
    input_3: " 900",
    expected_output: "([STRUCT(CAST('2020-01-01 01:00:00 UTC' AS TIMESTAMP) AS window_start, CAST('2020-01-01 01:15:00 UTC' AS TIMESTAMP) AS window_end)])",
  },
]);
generate_udf_test("nlp_compromise_number", [
  {
    input_1: "CAST('one hundred fifty seven' AS STRING)",
    expected_output: "CAST(157 AS NUMERIC)",
  },
  {
    input_1: "CAST('three point 5' AS STRING)",
    expected_output: "CAST(3.5 AS NUMERIC)",
  },
  {
    input_1: "CAST('2 hundred' AS STRING)",
    expected_output: "CAST(200 AS NUMERIC)",
  },
  {
    input_1: "CAST('minus 8' AS STRING)",
    expected_output: "CAST(-8 AS NUMERIC)",
  },
  {
    input_1: "CAST('5 million 3 hundred 25 point zero 1' AS STRING)",
    expected_output: "CAST(5000325.01 AS NUMERIC)",
  },
]);
generate_udf_test("nlp_compromise_people", [
  // Below tests taken from https: "//github.com/spencermountain/compromise/blob/master/tests/people.test.js",
  {
    input_1: "CAST('Mary is in the boat. Nancy is in the boat. Fred is in the boat. Jack is too.' AS STRING)",
    expected_output: "CAST(['mary', 'nancy', 'fred', 'jack'] AS ARRAY<STRING>)",
  },
  {
    input_1: "CAST('jean jacket. jean Slkje' AS STRING)",
    expected_output: "CAST(['jean slkje'] AS ARRAY<STRING>)",
  },
  {
    input_1: "CAST('The Bill was passed by James MacCarthur' AS STRING)",
    expected_output: "CAST(['james maccarthur'] AS ARRAY<STRING>)",
  },
  {
    input_1: "CAST('Rod MacDonald bought a Rod' AS STRING)",
    expected_output: "CAST(['rod macdonald'] AS ARRAY<STRING>)",
  },
  {
    input_1: "CAST(\"Matt 'the doctor' Smith lasted three seasons.\" AS STRING)",
    expected_output: '  CAST(["matt the doctor smith"] AS ARRAY<STRING>)',
  },
  {
    input_1: "CAST(\"Randal Kieth Orton and Dwayne 'the rock' Johnson had a really funny fight.\" AS STRING)",
    expected_output: "CAST(['randal kieth orton', 'dwayne the rock johnson'] AS ARRAY<STRING>)",
  },
]);
generate_udf_test("levenshtein", [
  {
    input_1: "CAST('analyze' AS STRING)",
    input_2: " CAST('analyse' AS STRING)",
    expected_output: "CAST(1 AS INT64)",
  },
  {
    input_1: "CAST('opossum' AS STRING)",
    input_2: " CAST('possum' AS STRING)",
    expected_output: "CAST(1 AS INT64)",
  },
  {
    input_1: "CAST('potatoe' AS STRING)",
    input_2: " CAST('potatoe' AS STRING)",
    expected_output: "CAST(0 AS INT64)",
  },
  {
    input_1: "CAST('while' AS STRING)",
    input_2: " CAST('whilst' AS STRING)",
    expected_output: "CAST(2 AS INT64)",
  },
  {
    input_1: "CAST('aluminum' AS STRING)",
    input_2: " CAST('alumininium' AS STRING)",
    expected_output: "CAST(3 AS INT64)",
  },
  {
    input_1: "CAST('Connecticut' AS STRING)",
    input_2: " CAST('CT' AS STRING)",
    expected_output: "CAST(10 AS INT64)",
  },
]);
//
//  Below targets StatsLib work
//
generate_udf_test("pvalue", [
  {
    input_1: "CAST(0.3 AS FLOAT64)",
    input_2: "CAST(2 AS INT64)",
    expected_output: "CAST(0.8607079764250578 AS FLOAT64)",
  },
]);
generate_udf_test("kruskal_wallis", [
  {
    input_1: "(SELECT [('a',1.0), ('b',2.0), ('c',2.3), ('a',1.4), ('b',2.2), ('c',5.5), ('a',1.0), ('b',2.3), ('c',2.3), ('a',1.1), ('b',7.2), ('c',2.8)])",
    expected_output: "STRUCT(CAST(3.423076923076927 AS FLOAT64) AS H, CAST( 0.1805877514841956 AS FLOAT64) AS p, CAST(2 AS INT64) AS DoF)",
  },
]);
generate_udf_test("linear_regression", [
  {
    input_1: "(SELECT [ (5.1,2.5), (5.0,2.0), (5.7,2.6), (6.0,2.2), (5.8,2.6), (5.5,2.3), (6.1,2.8), (5.5,2.5), (6.4,3.2), (5.6,3.0)])",
    expected_output: "STRUCT(CAST(-0.4353361094588436 AS FLOAT64) AS a, CAST( 0.5300416418798544 AS FLOAT64) AS b, CAST(0.632366563565354 AS FLOAT64) AS r)",
  },
]);
