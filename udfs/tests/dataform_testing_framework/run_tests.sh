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

clean(){
  rm -rf node_modules
  rm -f package-lock.json
  rm -f .df-credentials.json
  rm -r dataform.json
  rm -rf dataform_udfs_temp_deploy
  rm -rf dataform_udf_unit_tests
  bq --project_id "${PROJECT_ID}" rm -r -f --dataset fn
}

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
    sed -i "s|\${JS_BUCKET}|${js_bucket}|g" "${file}"
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
  ln -sf "$(pwd)"/includes/ "${target_dir}"/includes
}

generate_dataform_config_and_creds(){
  export PROJECT_ID=$1
  export DATASET_ID=$2
  local target_dir=$3
  envsubst < dataform_template.json > "${target_dir}"/dataform.json
  # Create an .df-credentials.json file as shown below
  # in order to have Dataform pick up application default credentials
  # https://cloud.google.com/docs/authentication/production#automatically
  echo "{\"projectId\": \"${project_id}\", \"location\": \"${BQ_LOCATION}\"}" > "${target_dir}"/.df-credentials.json
}

deploy_udfs(){
  local project_id=$1
  local dataset_id=$2
  local udfs_source_dir=$3
  local udfs_target_dir=$4
  mkdir -p "${udfs_target_dir}"/definitions
  mv "${udfs_source_dir}"/test_cases.js "${udfs_source_dir}"/test_cases.js.ignore
  ln -sf "${udfs_source_dir}"/ "${udfs_target_dir}"/definitions/community
  replace_js_udf_bucket_placeholder "${udfs_source_dir}" gs://dannybq/test_bq_js_libs
  generate_dataform_config_and_creds "${project_id}" "${dataset_id}" "${udfs_target_dir}"
  add_symbolic_dataform_dependencies "${udfs_target_dir}"
  dataform run "${udfs_target_dir}"
}

test_udfs(){
  local project_id=$1
  local dataset_id=$2
  local udfs_source_dir=$3
  local udfs_target_dir=$4
  mkdir -p "${udfs_target_dir}"/definitions
  cp "${udfs_source_dir}"/test_cases.js.ignore "${udfs_target_dir}"/definitions/test_cases.js
  generate_dataform_config_and_creds "${project_id}" "${dataset_id}" "${udfs_target_dir}"
  add_symbolic_dataform_dependencies "${udfs_target_dir}"
  dataform test "${udfs_target_dir}"
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
  clean

  # Create an empty dataform.json file because Dataform requires
  # this file's existence when installing dependencies.
  touch dataform.json
  # Only run 'dataform install' once, symbolic links will be used
  # for all other Dataform project directories.
  dataform install > /dev/null 2>&1

  deploy_udfs ${PROJECT_ID} ${DATASET_ID} "$(pwd)"/../../../udfs/community dataform_udfs_temp_deploy
  test_udfs ${PROJECT_ID} ${DATASET_ID} "$(pwd)"/../../../udfs/community dataform_udf_unit_tests
  clean
}

main
