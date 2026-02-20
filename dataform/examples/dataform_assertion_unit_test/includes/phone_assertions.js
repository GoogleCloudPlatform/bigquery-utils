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

/*
    This assertion checks that the the input telephone number does not begin with 0
*/
function test_telephone_number_start_with_zero(colName) {
  var result_query = `SUBSTRING(${colName},1,1) != '0'`
  return result_query
}

/*
    This assertion checks that the input telephone number does not contain more than all repeated digits
*/
function test_repeated_phone_number(colName) {
  var result_query = `TRIM(${colName}, '0') != "" AND ` +
      `TRIM(${colName}, '1') != "" AND ` +
      `TRIM(${colName}, '2') != "" AND ` +
      `TRIM(${colName}, '3') != "" AND ` +
      `TRIM(${colName}, '4') != "" AND ` +
      `TRIM(${colName}, '5') != "" AND ` +
      `TRIM(${colName}, '6') != "" AND ` +
      `TRIM(${colName}, '7') != "" AND ` +
      `TRIM(${colName}, '8') != "" AND ` +
      `TRIM(${colName}, '9') != ""`
  return result_query
}

/*
    This assertion checks that the telephone only contains 10 digits
*/
function test_only_contain_ten_digit(colName) {
  var result_query = `REGEXP_CONTAINS(${colName}, r'\\d{10}')`
  return result_query
}

/*
    This assertions combines the above assertions
*/
function test_phone_number(colName) {
  var result_query = `${test_telephone_number_start_with_zero(colName)} AND ` +
      `${test_repeated_phone_number(colName)} AND ${test_only_contain_ten_digit(colName)}`
  return result_query
}

module.exports = {
  test_telephone_number_start_with_zero,
  test_repeated_phone_number,
  test_only_contain_ten_digit,
  test_phone_number,
}
