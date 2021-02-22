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
const {test_phone_number} = phone_assertions;
const test_name = "test_telephone_number_assertions";
const test_cases = {
    /*
        Provide your own testing data following the structure
        <INPUT_TESTING_DATA> : "<EXPECTED OUTCOME>"
        For example, if a testing data has the <EXPECTED OUTCOME> to be TRUE,
        then the program will expect the custom data quality rules to also produce TRUE. 
        Otherwise it will show that the custom data quality rules failed. 
    */
    
   "8123456789" : "TRUE",
   "1234567899" : "TRUE",
   "5123456789" : "TRUE",
   "4576839485" : "TRUE",
   "2938475638" : "TRUE",
   "7928374657" : "TRUE",
   "7847563738" : "TRUE",
   "6768907654" : "TRUE",
   "1234567" : "FALSE",
   "0123456789" : "FALSE",
   "1111111111" : "FALSE",
   "374657389a" : "FALSE"
};
// The function below will generate the necessary SQL to run unit tests.
generate_test(test_name,
    test_cases,
    test_phone_number);
    