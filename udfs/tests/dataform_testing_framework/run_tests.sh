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
    rm -f "${file}.bak"
  done <<<"$(find "${udf_dir}" -type f -name "*.sqlx")"
}

copy_sql_and_rename_to_sqlx() {
  local udf_dir=$1
  local destination
  local newfilename
  while read -r file; do
    newfilename=$(basename "${file}" | cut -f 1 -d '.')
    destination="../../../udfs/community/${newfilename}.sqlx"
    printf "Copying file %s to %s\n" "$file" "$destination"
    mv "${file}" "${destination}"
  done <<<"$(find "${udf_dir}" -type f -name "*.sql")"
}

add_symbolic_dataform_dependencies(){
  local target_dir=$1
  # Create symbolic links to dataform config files and node_modules
  # to save time and not duplicate resources
  ln -sf "$(pwd)"/package.json "${target_dir}"/package.json
  ln -sf "$(pwd)"/node_modules/ "${target_dir}"/node_modules
}

generate_dataform_credentials(){
  local project_id=$1
  local dataform_dir=$2
  # Create an .df-credentials.json file as shown below
  # in order to have Dataform pick up application default credentials
  # https://cloud.google.com/docs/authentication/production#automatically
  echo "{\"projectId\": \"${project_id}\", \"location\": \"${BQ_LOCATION}\"}" > "${dataform_dir}"/.df-credentials.json
}

generate_dataform_config_and_creds(){
  export PROJECT_ID=$1
  export DATASET_ID=$2
  envsubst < dataform_dev.json > dataform.json
  generate_dataform_credentials "${PROJECT_ID}" .
  #redirect to null to avoid noise
  dataform install > /dev/null 2>&1
}

set_env_vars(){
  # For now, this build script assumes all BigQuery environments
  # live in the same location which you specify below.
  export BQ_LOCATION=US

  # PROD project points to the live BigQuery environment
  # which must be kept in sync with DDL changes.
  export PROJECT_ID=danny-bq
  export DATASET_ID=dataform
}


main() {
  set_env_vars
  generate_dataform_credentials "${PROJECT_ID}" .
  dataform install > /dev/null 2>&1

  mkdir -p dataform_udfs_temp/definitions
  ln -sf "$(pwd)"/dataform.json dataform_udfs_temp/dataform.json
  add_symbolic_dataform_dependencies dataform_udfs_temp

  ln -sf "$(pwd)"/.df-credentials.json dataform_udfs_temp/.df-credentials.json
  ln -sf "$(pwd)"/../../../udfs/community/ dataform_udfs_temp/definitions

  ln -sf "$(pwd)"/dataform.json dataform_udf_unit_tests/dataform.json
  add_symbolic_dataform_dependencies dataform_udf_unit_tests
  ln -sf "$(pwd)"/.df-credentials.json dataform_udf_unit_tests/.df-credentials.json

  #  copy_sql_and_rename_to_sqlx ../../../udfs/community
  replace_js_udf_bucket_placeholder ../../../udfs/community gs://dannybq/test_bq_js_libs
  dataform run dataform_udfs_temp/
  dataform test dataform_udf_unit_tests/
  rm -rf dataform_udfs_temp
}

main
