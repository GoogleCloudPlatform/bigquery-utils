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

generate_udf_test("convert_string_bytes", [
  {
    inputs: [`'64617461'`, `0`],
    expected_output: `FROM_HEX('3634363137343631')`
  },
  {
    inputs: [`'0x64617461'`, `1`],
    expected_output: `FROM_HEX('64617461')`
  },
  {
    inputs: [`'64617461'`,  `2`],
    expected_output: `FROM_HEX('64617461')`
  },
]);

generate_udf_test("convert_bytes_string", [
  {
    inputs: [`FROM_HEX('64617461')`, `0`],
    expected_output: `'data'`
  },
  {
    inputs: [`FROM_HEX('64617461')`, `1`],
    expected_output: `'0x64617461'`
  },
  {
    inputs: [`FROM_HEX('64617461')`, `2`],
    expected_output: `'64617461'`
  },
]);

generate_udf_test("convert_datetime_string", [
  {
    inputs: [`DATETIME '2024-10-01 13:34:56.789'`, `130`],
    expected_output: `'01 Oct 2024 01:34:56:7890000PM'`,
  },
  {
    inputs: [`DATETIME '2024-10-01 13:34:56.789'`, `1`],
    expected_output: `'10/01/24'`,
  },
]);

generate_udf_test("convert_timestamp_string", [
  {
    inputs: [`TIMESTAMP '2024-10-01 12:34:56.789'`, `127`],
    expected_output: `'2024-10-01T12:34:56.7890000Z'`,
  },
  {
    inputs: [`TIMESTAMP '2024-10-01 13:34:56.789'`, `1`],
    expected_output: `'10/01/24'`,
  },
]);

generate_udf_test("convert_numeric_string", [
  {
    inputs: [`123.456789`, `0`],
    expected_output: `'123.457'`,
  },
  {
    inputs: [`123.456789`, `1`],
    expected_output: `'1.234568e+02'`,
  },
  {
    inputs: [`123.456789`, `2`],
    expected_output: `'    1.234568e+02'`,
  },
  {
    inputs: [`123.456789`, `3`],
    expected_output: `'       123.456789'`,
  },
]);
