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

function generate_udf_test(test_udf, test_cases) {
  const test_name = `${test_udf}_${uuidv4()}`;
  create_dataform_test_view(test_name, test_udf, test_cases);
  let expected_output_select_statements = [];
  let test_input_select_statements = [];
  test_cases.forEach((test_case) => {
    const keys = Object.keys(test_case);
    let inputs = "";
    keys.forEach((key_name) => {
      if (key_name.startsWith("input")) {
        inputs += `${test_case[key_name]} AS test_${key_name},\n`;
      }
    });
    test_input_select_statements.push(`
          SELECT ${inputs}
        `);
    expected_output_select_statements.push(`
          SELECT ${test_case.expected_output} AS udf_output
        `);
  });
  run_dataform_test(
    test_name,
    test_input_select_statements,
    expected_output_select_statements
  );
}

function create_dataform_test_view(test_name, test_udf, test_cases) {
  const keys = Object.keys(test_cases[0]);
  let udf_input_aliases = "";
  keys.forEach((key_name) => {
    if (key_name.startsWith("input")) {
      udf_input_aliases += `test_${key_name},`;
    }
  });
  udf_input_aliases = udf_input_aliases.slice(0, udf_input_aliases.length - 1);
  publish(test_name)
    .type("view")
    .query(
      (ctx) => `
            SELECT
              ${get_udf_project_and_dataset()}.${test_udf}(
                    ${udf_input_aliases}) AS udf_output
            FROM ${ctx.resolve("test_inputs")}
        `
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
      "test_inputs",
      `${test_input_select_statements.join(" UNION ALL\n")}`
    )
    .expect(`${expected_output_select_statements.join(" UNION ALL\n")}`);
}

function get_udf_project_and_dataset() {
  // This function returns the default BigQuery project and
  // dataset which are specified in the dataform.json config file.
  return `\`${dataform.projectConfig.defaultDatabase}.${dataform.projectConfig.defaultSchema}\``;
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
};
