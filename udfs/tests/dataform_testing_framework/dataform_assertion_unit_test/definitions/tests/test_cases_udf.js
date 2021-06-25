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

const {
  generate_test_udf_1_input,
  generate_test_udf_2_inputs,
  generate_test_udf_3_inputs,
} = unit_test_utils;

generate_test_udf_2_inputs("fn.url_parse", [
    {
      "input_1": "CAST('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' AS STRING)",
      "input_2": "CAST('HOST' AS STRING)",
      "expected_output": "CAST('facebook.com' AS STRING)"
    },{
      "input_1": "CAST('rpc://facebook.com/' AS STRING)",
      "input_2": "CAST('HOST' AS STRING)",
      "expected_output": "CAST('facebook.com' AS STRING)"
    }
]);

generate_test_udf_1_input("fn.int", [
    {
      "input_1":"CAST(2.5 AS FLOAT64)",
      "expected_output": "CAST(2 AS INT64)"
    },{
      "input_1": "CAST(7.8 AS FLOAT64)",
      "expected_output": "CAST(7 AS INT64)"
    }
]);

generate_test_udf_1_input("fn.int", [
    {
      "input_1": "CAST(-1 AS STRING)",
      "expected_output": "CAST(-1 AS INT64)"
    }
]);
