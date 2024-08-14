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

function generate_udf_test(udf_name, test_cases) {
    const test_name = `${udf_name}_${uuidv4()}`;
    create_dataform_test_view(test_name, udf_name, test_cases);
    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    test_cases.forEach((test_case) => {
        let udf_positional_inputs = [];
        test_case.inputs.forEach((input, index) => {
            udf_positional_inputs.push(`${input} AS test_input_${index}`);
        });
        test_input_select_statements.push(`\n  SELECT ${udf_positional_inputs.join(', ')}`);
        expected_output_select_statements.push(`SELECT ${test_case.expected_output} AS udf_output`);
    });
    run_dataform_test(
        test_name,
        test_input_select_statements,
        expected_output_select_statements
    );
}

function generate_udaf_test(udaf_name, test_case) {
    const test_name = `${udaf_name}_${uuidv4()}`;
    create_dataform_udaf_test_view(test_name, udaf_name, test_case);
    let expected_output_select_statements = [];
    let test_input_select_statements = [];
    let udf_positional_inputs = [];
    test_case.input_columns.forEach((input, index) => {
        if (!input.includes(' NOT AGGREGATE')) {
            udf_positional_inputs.push(`${input} AS test_input_${index}`);
        }
    });
    test_input_select_statements.push(`\n  SELECT ${udf_positional_inputs.join(', ')} FROM (${test_case.input_rows})`);
    expected_output_select_statements.push(`SELECT ${test_case.expected_output} AS udf_output`);
    run_dataform_test(
        test_name,
        test_input_select_statements,
        expected_output_select_statements
    );
}

function create_dataform_test_view(test_name, udf_name, test_cases) {
    const inputs = Object.keys(test_cases[0].inputs);
    let udf_input_aliases = [];
    inputs.forEach((input, index) => {
        udf_input_aliases.push(`test_input_${index}`);
    });
    udf_input_aliases = udf_input_aliases.join(',');
    const udf_invocation_str = `${get_udf_project_and_dataset(udf_name)}${udf_name}(${udf_input_aliases})`;
    publish(`${test_name}_dummy_view`).type("view").query("SELECT 1 as col1");
    publish(test_name)
        .type("view")
        .query(
            (ctx) => `SELECT ${udf_invocation_str} AS udf_output\n` +
                `FROM ${ctx.resolve(`${test_name}_dummy_view`)}`
        );
}

function create_dataform_udaf_test_view(test_name, udf_name, test_case) {
    let udf_input_aliases = [];
    test_case.input_columns.forEach((input, index) => {
        if (input.includes(' NOT AGGREGATE')) {
            udf_input_aliases.push(`${input.split(' NOT AGGREGATE')[0]}`);
        } else {
            udf_input_aliases.push(`test_input_${index}`);
        }
    });
    udf_input_aliases = udf_input_aliases.join(',');
    const udf_invocation_str = `${get_udf_project_and_dataset(udf_name)}${udf_name}(${udf_input_aliases})`;
    publish(`${test_name}_dummy_view`).type("view").query("SELECT 1 as col1");
    publish(test_name)
        .type("view")
        .query(
            (ctx) => `SELECT ${udf_invocation_str} AS udf_output\n` +
                `FROM ${ctx.resolve(`${test_name}_dummy_view`)}`
        );
}

function run_dataform_test(
    test_name,
    test_input_select_statements,
    expected_output_select_statements
) {
    test(test_name)
        .dataset(test_name)
        .input(
            `${test_name}_dummy_view`,
            `${test_input_select_statements.join("\n  UNION ALL")}`
        )
        .expect(`${expected_output_select_statements.join("\nUNION ALL\n")}`);
}

function get_udf_project_and_dataset(udf_name) {
    // This function returns either a missing project_id or dataset_id
    // from the user-provided udf_name. Any missing IDs are added using data
    // from the dataform.json config file.
    const regexp = /\./g; // Check for periods in udf_name
    const matches = [...udf_name.matchAll(regexp)];
    if (matches.length === 0) {
        // No periods in udf_name means project and dataset must be added
        // for a fully-qualified UDF invocation.
        return `\`${dataform.projectConfig.defaultDatabase}.${dataform.projectConfig.defaultSchema}\`.`;
    } else if (matches.length === 1) {
        // Only one period in udf_name means the project must be added
        // for a fully-qualified UDF invocation.
        return `\`${dataform.projectConfig.defaultDatabase}\`.`;
    } else if (matches.length === 2) {
        // Two periods in the udf_name means the user has already provided
        // both project and dataset. No change is necessary.
        return '';
    }
}

// Source: https://stackoverflow.com/a/2117523
function uuidv4() {
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
        var r = (Math.random() * 16) | 0,
            v = c == "x" ? r : (r & 0x3) | 0x8;
        return v.toString(16);
    });
}

module.exports = {
    generate_udf_test,
    generate_udaf_test,
};
