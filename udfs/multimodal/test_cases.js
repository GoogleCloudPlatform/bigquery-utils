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

generate_udf_test("exif", [
  {
    inputs: [`{"access_urls":{"expiry_time":"2025-06-06T03:55:18Z","read_url":"https://storage.googleapis.com/bigframes_blob_test/images_exif%2Ftest_image_exif.jpg?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=bqcx-1084210331973-pcbl%40gcp-sa-bigquery-condel.iam.gserviceaccount.com%2F20250605%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250605T215518Z&X-Goog-Expires=21600&X-Goog-SignedHeaders=host&X-Goog-Signature=a37d6a9cea9ef82f04ca81e05f56b38936c6651d026b8fd460e69488eecd212c95a800a3dc1c5403a78f3d40616624d0cce1c1b46baf5416593dce355872ebb95ee3f0ded6009866927c0f05f25d5b551d058c97f5d9ef7bd3e8e9fbc59e500a4f7a69d429cc7d0f985dae5529ae7befd17ca3d11c79f4fb0b241d2da339bc74ef09e70a3349e19de7688689c1e9b8b45df12af65ef7263ec5de4f577458f061c0a1d38496f209afc45d69d05d845ab305c3d65066acd67fc17e950977ff610babf463840cf2f3c12211e743132637a1056ed8b41a9a7005cbe0e6740986e9fad6f4eee602527aa37d8963d30ce9c76f9bc2062e2927a6cadbad6a840d4c9ff7","write_url":""},"objectref":{"authorizer":"bigframes-dev.us.bigframes-default-connection","uri":"gs://bigframes_blob_test/images_exif/test_image_exif.jpg"}}`],
    expected_output: `{"ExifOffset": 47, "Make": "MyCamera"}`
  }
]);
