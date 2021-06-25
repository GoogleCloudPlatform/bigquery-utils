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

const {generate_test} = unit_test_utils;
const {test_int_udf} = udf_assertions;

let test_name = "test_int_udf_with_int64_inputs";
let test_cases_int64_inputs = {
    /*
        Provide your own testing data following the structure
        <INPUT_TESTING_DATA> : "<EXPECTED OUTCOME>"
        For example, if a testing data has the <EXPECTED OUTCOME> to be TRUE,
        then the program will expect the custom data quality rules to also produce TRUE.
        Otherwise it will show that the custom data quality rules failed.
    */
    "CAST(1 AS INT64)"     : "CAST(1 AS INT64)",
    "CAST(7 AS INT64)"     : "CAST(7 AS INT64)",
};
// The function below will generate the necessary SQL to run unit tests.
generate_test(test_name,
    test_cases_int64_inputs,
    test_int_udf);

test_name = "test_int_udf_with_float64_inputs";
const test_cases_float64_inputs = {
    /*
        Provide your own testing data following the structure
        <INPUT_TESTING_DATA> : "<EXPECTED OUTCOME>"
        For example, if a testing data has the <EXPECTED OUTCOME> to be TRUE,
        then the program will expect the custom data quality rules to also produce TRUE.
        Otherwise it will show that the custom data quality rules failed.
    */
    "CAST(2.5 AS FLOAT64)" : "CAST(2 AS INT64)",
    "CAST(7.8 AS FLOAT64)" : "CAST(7 AS INT64)"
};
// The function below will generate the necessary SQL to run unit tests.
generate_test(test_name,
    test_cases_float64_inputs,
    test_int_udf);

test_name = "test_int_udf_with_string_inputs";
const test_cases_string_inputs = {
    /*
        Provide your own testing data following the structure
        <INPUT_TESTING_DATA> : "<EXPECTED OUTCOME>"
        For example, if a testing data has the <EXPECTED OUTCOME> to be TRUE,
        then the program will expect the custom data quality rules to also produce TRUE.
        Otherwise it will show that the custom data quality rules failed.
    */
    "CAST(-1 AS STRING)"   : "CAST(-1 AS INT64)",
};
// The function below will generate the necessary SQL to run unit tests.
generate_test(test_name,
    test_cases_string_inputs,
    test_int_udf);
