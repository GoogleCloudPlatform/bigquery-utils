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
    This assertion checks whether input date is future
*/
function test_future_date(colName){
    var result_query = `PARSE_DATE('%Y/%m/%d', ${colName}) < CURRENT_DATE()`
    return result_query
}

/*
    This assertion checks whether the input birthdate is less than 100 yrs old
*/
function test_valid_years(colName){
    var result_query = `DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y/%m/%d', ${colName}), YEAR) < 100`
    return result_query
}

/*
    This function checks whether the format of  the date is correct
*/
function test_date_format(colName, date_format){
        if(date_format == "yyyy/mm/dd"){
            var result_query = `REGEXP_CONTAINS(${colName}, r'^[0-9]{4}[/][0-9]{2}[/][0-9]{2}$')`
            return result_query
        } else if (date_format == "yyyymmdd"){
            var result_query = `REGEXP_CONTAINS(${colName}, r'^[0-9]{4}[0-9]{2}[0-9]{2}$')`
            return result_query
        }else{
            return `FALSE`
        }
}

/*
    This assertions combines custom assertions for testing future date and valid years
*/

function test_date(colName){
    var result_query =
    `IF(${colName} IS NOT NULL AND ${colName} <> "",` +
        `IF(${test_date_format(colName, "yyyy/mm/dd")}, ` +
            `IF(${test_future_date(colName)}, ` +
                `${test_valid_years(colName, 100)}` +
                `, FALSE),` +
            `IF(${test_date_format(colName, "yyyymmdd")}, ` +
            `TRUE, FALSE)), FALSE)`
    return result_query
}

module.exports = {
    test_future_date,
    test_valid_years,
    test_date_format,
    test_date
}
