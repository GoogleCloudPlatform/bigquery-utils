// Copyright 2024 Google LLC
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
    This test will return FALSE if telephone number meets any of the following conditions:
    -  Contains different digits from 0 to 9.
    - Not 10 length positions
    - Start with a zero.
    - If the phone number starts with a ""55"", exclude the ""55"" at the beginning and validate there is one of the following series: ""000000"", ""111111"", ""222222"", ""333333"", ""444444"", ""555555"", ""666666"", ""777777"", ""888888"", ""999999"",""123456""
    - Is assigned to more than 4 policies from diferent customer.
    -Match with any number in the ""Frequent case"" list.
    - In a 10 digit phone number, validate from left to right the long distance code, series and type:

    LADA / Long distance code
     If it starts with 55, 52, 33 or 81, valdiate the LADA with 2 positions, otherwise, validate it with 3 positions.

    SERIE / SERIES
    If the LADA had 2 positions, the series must have 4 digits; if the lada had 3 positions, the Lada must have the next 3 positions.

    TIPO / TYPE
    The value ""TIPO DE RED"" (network type) in the catalog indicates whether the number is mobile or land line.

    (URL for National dialing plan : https://sns.ift.org.mx:8081/sns-frontend/planes-numeracion/descarga-publica.xhtm)
*/
function test_phone_number_validity(colName) {
  var remSplCharsLeadingZeros = `${test_remove_leading_zeros(
    test_remove_special_chars(colName)
  )}`;

  return `${test_phone_number_contain_digit(remSplCharsLeadingZeros)}
           AND ${test_repeated_phone_number(remSplCharsLeadingZeros)}`;
}

/*
    Remove special characters
    . : , ; ! " # $ % & / ( ) = ' +
*/
function test_remove_special_chars(colName) {
  var no_whitespace = `REPLACE(${colName}, ' ', '')`;
  var no_dot = `REPLACE(${no_whitespace}, '.', '')`;
  var no_colon = `REPLACE(${no_dot}, ':', '')`;
  var no_comma = `REPLACE(${no_colon}, ',', '')`;
  var no_semicolon = `REPLACE(${no_comma}, ';', '')`;
  var no_exclamation = `REPLACE(${no_semicolon}, '!', '')`;
  var no_double_quote = `REPLACE(${no_exclamation}, '"', '')`;
  var no_hash = `REPLACE(${no_double_quote}, '#', '')`;
  var no_dollar = `REPLACE(${no_hash}, '$', '')`;
  var no_percentage = `REPLACE(${no_dollar}, '%', '')`;
  var no_ampersand = `REPLACE(${no_percentage}, '&', '')`;
  var no_forward_slash = `REPLACE(${no_ampersand}, '/', '')`;
  var no_left_parenthesis = `REPLACE(${no_forward_slash}, '(', '')`;
  var no_right_parenthesis = `REPLACE(${no_left_parenthesis}, ')', '')`;
  var no_equal = `REPLACE(${no_right_parenthesis}, '=', '')`;
  var no_single_quote = `REPLACE(${no_equal}, '\\'', '')`;
  var no_plus = `REPLACE(${no_single_quote}, '+', '')`;
  return no_plus;
}

/*
    Precondition leading zero removal
    If starts with "00055", delete the "000"
*/
function test_remove_leading_zeros(colName) {
  return `LTRIM(${colName}, '0')`;
}

/*
    This assertion checks that the input telephone number does not contain more than 3 repeated digits
*/
function test_repeated_phone_number(colName) {
  return `TRIM( ${colName}, "0") != "" AND
            TRIM( ${colName}, "1") != "" AND
            TRIM( ${colName}, "2") != "" AND
            TRIM( ${colName}, "3") != "" AND
            TRIM( ${colName}, "4") != "" AND
            TRIM( ${colName}, "5") != "" AND
            TRIM( ${colName}, "6") != "" AND
            TRIM( ${colName}, "7") != "" AND
            TRIM( ${colName}, "8") != "" AND
            TRIM( ${colName}, "9") != ""`;
}

/*
    This assertion checks that the telephone number only contains digits & length not greater than 10
*/
function test_phone_number_contain_digit(colName) {
  return `REGEXP_CONTAINS(${colName}, r'^[1-9]{1}\\d{9}$')`;
}

function test_phone_number(colName) {
  return (
    `${test_phone_number_validity(colName)}` +
    `AND ${test_phone_number_contain_digit(colName)}` +
    //   `AND ${test_remove_special_chars(colName)}` +
    //   `AND ${test_remove_leading_zeros(colName)}` +
    `AND ${test_repeated_phone_number(colName)}` +
    `AND ${test_phone_number_contain_digit(colName)}`
  );
}

module.exports = {
  test_phone_number,
};
