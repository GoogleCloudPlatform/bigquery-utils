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
This assertion checks whether the input email format is valid
*/
function test_email_validity(colName){
    var result_query = `REGEXP_CONTAINS(${colName}, r'^[\\w.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$')`
    return result_query
}

/*
    This assertion checks whether the input marital status is within an acceptable list 
*/
function test_marital_status(colName){
    var marital_list = "'Married', 'Single', 'Divorced', 'Widowed'"
    var result_query = `${colName} IN(${marital_list})`
    return result_query
}

/*
    This assertion checks whether the input gender status is within an acceptable list
*/
function test_gender_status(colName){
    var gender_list = "'Female','Male','Transgender Female','Transgender Male','Gender Variant','Prefer Not to Say'"
    var result_query = `${colName} IN (${gender_list})`
    return result_query
}

/*
    This assertion checks whether the name is valid and only contain characters and numbers
*/
function test_name_validity(colName){
    var result_query = `REGEXP_CONTAINS(${colName}, r'^[a-zA-Z]+$')`
    return result_query
}

/*
    This assertions compares with other input column to check whether the last name is unique
*/
function test_last_name_unique(colName1, colName2){
    var result_query = `${colName1} != ${colName2}`
    return result_query
}

/*
    The assertion checks that no name contain more than n repeated characters 
*/
function test_same_character_not_more_than_n_times(colName, n_times){
    var regex = `(A{${n_times + 1},})+|` +
                `(B{${n_times + 1},})+|` +
                `(C{${n_times + 1},})+|` +
                `(D{${n_times + 1},})+|` +
                `(E{${n_times + 1},})+|` +
                `(F{${n_times + 1},})+|` +
                `(G{${n_times + 1},})+|` +
                `(H{${n_times + 1},})+|` +
                `(I{${n_times + 1},})+|` +
                `(J{${n_times + 1},})+|` +
                `(K{${n_times + 1},})+|` +
                `(L{${n_times + 1},})+|` +
                `(M{${n_times + 1},})+|` +
                `(N{${n_times + 1},})+|` +
                `(O{${n_times + 1},})+|` +
                `(P{${n_times + 1},})+|` +
                `(Q{${n_times + 1},})+|` +
                `(R{${n_times + 1},})+|` +
                `(S{${n_times + 1},})+|` +
                `(T{${n_times + 1},})+|` +
                `(U{${n_times + 1},})+|` +
                `(V{${n_times + 1},})+|` +
                `(W{${n_times + 1},})+|` +
                `(X{${n_times + 1},})+|` +
                `(Y{${n_times + 1},})+|` +
                `(Z{${n_times + 1},})+`
    var query_result = `NOT REGEXP_CONTAINS(UPPER(${colName}), r'${regex}')`
    return query_result
}

/*
    This assertions combines custom assertions for names
 */
function test_name(colName, n_times){
    var result_query = `${test_name_validity(colName)} AND ${test_same_character_not_more_than_n_times(colName, 3)}`
    return result_query
}

module.exports = {
    test_email_validity,
    test_marital_status,
    test_gender_status,
    test_name_validity,
    test_last_name_unique,
    test_same_character_not_more_than_n_times,
    test_name
}
