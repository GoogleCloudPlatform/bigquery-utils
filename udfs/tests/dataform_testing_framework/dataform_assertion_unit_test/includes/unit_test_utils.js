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

function generate_test_udf_1_input(test_udf, test_cases){
    const test_name = `${test_udf}_${uuidv4()}`
    publish(test_name)
        .type("view")
        .query(ctx => `
            SELECT
              ${get_udf_project()}.${test_udf}(
                    test_input_1) AS udf_output
            FROM ${ctx.resolve("test_inputs")}
        `);

    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    test_cases.forEach(test_case => {
        test_input_select_statements.push(`
          SELECT
            ${test_case.input_1} AS test_input_1,
        `);
        expected_output_select_statements.push(`
          SELECT ${test_case.expected_output} AS udf_output
        `);
    })
    run_dataform_test(test_name, test_input_select_statements, expected_output_select_statements)
}

function generate_test_udf_2_inputs(test_udf, test_cases){
    const test_name = `${test_udf}_${uuidv4()}`
    publish(test_name)
        .type("view")
        .query(ctx => `
            SELECT
              ${get_udf_project()}.${test_udf}(
                    test_input_1,
                    test_input_2) AS udf_output
            FROM ${ctx.resolve("test_inputs")}
        `);

    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    test_cases.forEach(test_case => {
        test_input_select_statements.push(`
          SELECT
            ${test_case.input_1} AS test_input_1,
            ${test_case.input_2} AS test_input_2,
        `);
        expected_output_select_statements.push(`
          SELECT ${test_case.expected_output} AS udf_output
        `);
    })
    run_dataform_test(test_name, test_input_select_statements, expected_output_select_statements)
}

function generate_test_udf_3_inputs(test_udf, test_cases){
    const test_name = `${test_udf}_${uuidv4()}`
    publish(test_name)
        .type("view")
        .query(ctx => `
            SELECT
              ${get_udf_project()}.${test_udf}(
                    test_input_1,
                    test_input_2,
                    test_input_3) AS udf_output
            FROM ${ctx.resolve("test_inputs")}
        `);

    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    test_cases.forEach(test_case => {
        test_input_select_statements.push(`
          SELECT
            ${test_case.input_1} AS test_input_1,
            ${test_case.input_2} AS test_input_2,
            ${test_case.input_3} AS test_input_3,
        `);
        expected_output_select_statements.push(`
          SELECT ${test_case.expected_output} AS udf_output
        `);
    })
    run_dataform_test(test_name, test_input_select_statements, expected_output_select_statements)
}

function generate_test_udf_4_inputs(test_udf, test_cases){
    const test_name = `${test_udf}_${uuidv4()}`
    publish(test_name)
        .type("view")
        .query(ctx => `
            SELECT
              ${get_udf_project()}.${test_udf}(
                    test_input_1,
                    test_input_2,
                    test_input_3,
                    test_input_4) AS udf_output
            FROM ${ctx.resolve("test_inputs")}
        `);

    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    test_cases.forEach(test_case => {
        test_input_select_statements.push(`
          SELECT
            ${test_case.input_1} AS test_input_1,
            ${test_case.input_2} AS test_input_2,
            ${test_case.input_3} AS test_input_3,
            ${test_case.input_4} AS test_input_4,
        `);
        expected_output_select_statements.push(`
          SELECT ${test_case.expected_output} AS udf_output
        `);
    })
    run_dataform_test(test_name, test_input_select_statements, expected_output_select_statements)
}

function generate_test_udf_5_inputs(test_udf, test_cases){
    const test_name = `${test_udf}_${uuidv4()}`
    publish(test_name)
        .type("view")
        .query(ctx => `
            SELECT
              ${get_udf_project()}.${test_udf}(
                    test_input_1,
                    test_input_2,
                    test_input_3,
                    test_input_4,
                    test_input_5) AS udf_output
            FROM ${ctx.resolve("test_inputs")}
        `);

    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    test_cases.forEach(test_case => {
        test_input_select_statements.push(`
          SELECT
            ${test_case.input_1} AS test_input_1,
            ${test_case.input_2} AS test_input_2,
            ${test_case.input_3} AS test_input_3,
            ${test_case.input_4} AS test_input_4,
            ${test_case.input_5} AS test_input_5,
        `);
        expected_output_select_statements.push(`
          SELECT ${test_case.expected_output} AS udf_output
        `);
    })
    run_dataform_test(test_name, test_input_select_statements, expected_output_select_statements)
}

function run_dataform_test(test_name, test_input_select_statements, expected_output_select_statements){
    test(test_name)
        .dataset(test_name)
        .input("test_inputs", `${test_input_select_statements.join(' UNION ALL\n')}`)
        .expect(`${expected_output_select_statements.join(' UNION ALL\n')}`);
}

function get_udf_project(){
  // This function returns the
  return `\`${dataform.projectConfig.defaultDatabase}\``
}

// Source: https://stackoverflow.com/a/2117523
function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

module.exports = {
    generate_test_udf_1_input,
    generate_test_udf_2_inputs,
    generate_test_udf_3_inputs,
    generate_test_udf_4_inputs,
    generate_test_udf_5_inputs,
}
