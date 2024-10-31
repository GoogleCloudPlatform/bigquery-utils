// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 "CAST(the "License");
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

const { generate_udf_test } = unit_test_utils;

generate_udf_test("round_datetime", [
  {
    inputs: [`DATETIME "2024-03-09"`, `"WW"`],
    expected_output: `DATETIME "2024-03-11T00:00:00"`
  },
  {
    inputs: [`DATETIME "2024-03-09"`, `"YEAR"`],
    expected_output: `DATETIME "2024-01-01T00:00:00"`
  },
  {
    inputs: [`DATETIME "2024-03-09"`, `"YEAR"`],
    expected_output: `DATETIME "2024-01-01T00:00:00"`
  },
  {
    inputs: [`DATETIME "2024-03-09 12:34:56"`, `"HH24"`],
    expected_output: `DATETIME "2024-03-09T13:00:00"`
  },
  {
    inputs: [`DATETIME "2024-3-4 11:34:56"`, `"W"`],
    expected_output: `DATETIME "2024-03-01T00:00:00"`
  },
  {
    inputs: [`DATETIME "2024-3-4 12:34:56"`, `"W"`],
    expected_output: `DATETIME "2024-03-08T00:00:00"`
  }
]);
