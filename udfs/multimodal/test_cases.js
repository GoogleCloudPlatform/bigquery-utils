// Copyright 2025 Google LLC
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

generate_udf_test("exif_udf", [
  {
    inputs: [`TO_JSON_STRING(OBJ.GET_ACCESS_URL(OBJ.MAKE_REF("${JS_BUCKET}/test_data/images_exif/test_image_exif.jpg", "${BQ_LOCATION}.bigframes-default-connection"), "R"))`],
    expected_output: `'{"ExifOffset": 47, "Make": "MyCamera"}'`
  }
]);


generate_udf_test("exif", [
  {
    inputs: [`OBJ.MAKE_REF("${JS_BUCKET}/test_data/images_exif/test_image_exif.jpg", "${BQ_LOCATION}.bigframes-default-connection")`],
    expected_output: `JSON '{"ExifOffset": 47, "Make": "MyCamera"}'`
  }
]);