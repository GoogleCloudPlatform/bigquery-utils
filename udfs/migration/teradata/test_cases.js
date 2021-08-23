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

const {generate_udf_test} = unit_test_utils;

generate_udf_test("last_day", [
  {
    input_1: 'DATE("2019-10-18")',
    expected_output: 'DATE("2019-10-31")',
  },
    {
      input_1: 'DATE("2019-11-18")',
      expected_output: 'DATE("2019-11-30")',
    },
    {
      input_1: 'DATE("2019-12-18")',
      expected_output: 'DATE("2019-12-31")',
    },
    {
      input_1: 'DATE("2019-01-18")',
      expected_output: 'DATE("2019-01-31")',
    },
    {
      input_1: 'DATE("2019-02-18")',
      expected_output: 'DATE("2019-02-28")',
    },
    {
      input_1: 'DATE("2019-03-18")',
      expected_output: 'DATE("2019-03-31")',
    },
    {
      input_1: 'DATE("2019-04-18")',
      expected_output: 'DATE("2019-04-30")',
    }
]);
generate_udf_test("last_day", [
  {
    input_1: 'DATE("2019-10-18")',
    expected_output: 'DATE("2019-10-31")',
  },
    {
      input_1: 'DATE("2019-11-18")',
      expected_output: 'DATE("2019-11-30")',
    },
    {
      input_1: 'DATE("2019-12-18")',
      expected_output: 'DATE("2019-12-31")',
    },
    {
      input_1: 'DATE("2019-01-18")',
      expected_output: 'DATE("2019-01-31")',
    },
    {
      input_1: 'DATE("2019-02-18")',
      expected_output: 'DATE("2019-02-28")',
    },
    {
      input_1: 'DATE("2019-03-18")',
      expected_output: 'DATE("2019-03-31")',
    },
    {
      input_1: 'DATE("2019-04-18")',
      expected_output: 'DATE("2019-04-30")',
    }
]);