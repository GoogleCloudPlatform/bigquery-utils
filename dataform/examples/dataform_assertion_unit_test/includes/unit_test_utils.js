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

function generate_test(test_name, test_cases, data_quality_function){
    publish(test_name)
        .type("view")
        .query(ctx => `
            SELECT
              ${data_quality_function("test_input")} AS is_valid
            FROM ${ctx.resolve("test_inputs")}
        `);

    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    for(var test_case in test_cases) {
        test_input_select_statements.push(`SELECT "${test_case}" AS test_input`);
        expected_output_select_statements.push(`SELECT ${test_cases[test_case]} AS is_valid`);
    };

    test(test_name)
        .dataset(test_name)
        .input("test_inputs", `${test_input_select_statements.join(' UNION ALL\n')}`)
        .expect(`${expected_output_select_statements.join(' UNION ALL\n')}`);
}

module.exports = {
    generate_test,
}
