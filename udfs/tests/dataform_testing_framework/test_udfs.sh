#!/bin/bash

# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#######################################
# Replaces all ${JS_BUCKET} placeholders
# in javascript UDFs with user-provided
# public hosting bucket.
# Arguments:
#   udf_dir
#   js_bucket
# Returns:
#   None
#######################################
replace_js_udf_bucket_placeholder() {
  # Replace all variable placeholders "${JS_BUCKET}" in Javascript UDFs
  # with the bucket that will host all javascript libraries
  local udf_dir=$1
  local js_bucket=$2
  printf "Replacing UDF bucket placeholder \${JS_BUCKET} with %s\n" "${js_bucket}"
  while read -r file; do
    printf "Replacing variables in file %s" "$file"
    sed -i.bak "s|\${JS_BUCKET}|${js_bucket}|g" "${file}"
  done <<<"$(find "${udf_dir}" -type f -name "*.sqlx")"
}

copy_sql_and_rename_to_sqlx() {
  local udf_dir=$1
  local destination
  while read -r file; do
    destination="dataform_udf_creation/definitions/community/$(basename "${file}").sqlx"
    printf "Copying file %s to %s\n" "$file" "$destination"
    cp "${file}" "${destination}"
  done <<<"$(find "${udf_dir}" -type f -name "*.sql")"
}

main() {
  echo '{"projectId": "", "location": "US"}' > .df-credentials.json
  mkdir -p dataform_udf_creation/definitions/community
  dataform install

  ln -sf "$(pwd)"/dataform.json dataform_udf_creation/dataform.json
  ln -sf "$(pwd)"/package.json dataform_udf_creation/package.json
  ln -sf "$(pwd)"/node_modules/ dataform_udf_creation/node_modules
  ln -sf "$(pwd)"/.df-credentials.json dataform_udf_creation/.df-credentials.json

  ln -sf "$(pwd)"/dataform.json dataform_assertion_unit_test/dataform.json
  ln -sf "$(pwd)"/package.json dataform_assertion_unit_test/package.json
  ln -sf "$(pwd)"/node_modules/ dataform_assertion_unit_test/node_modules
  ln -sf "$(pwd)"/.df-credentials.json dataform_assertion_unit_test/.df-credentials.json

  copy_sql_and_rename_to_sqlx ../../../udfs/community
  replace_js_udf_bucket_placeholder dataform_udf_creation/definitions/community gs://dannybq/test_bq_js_libs
  dataform run dataform_udf_creation/
  dataform test dataform_assertion_unit_test/
}

main