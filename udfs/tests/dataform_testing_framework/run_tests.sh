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
  rm -r dataform.json
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
  ln -sfn "$(pwd)"/node_modules/ "${target_dir}"/node_modules
  ln -sfn "$(pwd)"/includes/ "${target_dir}"/includes
}

generate_dataform_config_and_creds(){
  local project_id=$1
  local dataset_id=$2
  local target_dir=$3
  # Generate the dataform.json with the appropriate project_id and dataset_id
  sed "s|\${PROJECT_ID}|${project_id}|g" dataform_template.json \
  | sed "s|\${DATASET_ID}|${dataset_id}|g" > "${target_dir}"/dataform.json
  # Create an .df-credentials.json file as shown below
  # in order to have Dataform pick up application default credentials
  # https://cloud.google.com/docs/authentication/production#automatically
  echo "{\"projectId\": \"${project_id}\", \"location\": \"${BQ_LOCATION}\"}" > "${target_dir}"/.df-credentials.json
}

deploy_udfs(){
  local project_id=$1
  local js_bucket=$2
  local dataset_id=$3
  local short_dataset_id=$4
  local udfs_source_dir=$5
  local udfs_target_dir=$6
  mkdir -p "${udfs_target_dir}"/definitions
  # Copy all UDF sources into the target dir to avoid modifying the source itself
  cp -r "${udfs_source_dir}"/ "${udfs_target_dir}"/definitions/"${dataset_id}"
  # Temporarily rename test_cases.js to avoid deploying this file
  mv "${udfs_target_dir}"/definitions/"${dataset_id}"/test_cases.js \
     "${udfs_target_dir}"/definitions/"${dataset_id}"/test_cases.js.ignore
  replace_js_udf_bucket_placeholder "${udfs_target_dir}"/definitions/"${dataset_id}" "${js_bucket}"
  generate_dataform_config_and_creds "${project_id}" "${short_dataset_id}" "${udfs_target_dir}"
  add_symbolic_dataform_dependencies "${udfs_target_dir}"
  if ! dataform run "${udfs_target_dir}"; then
    # If any error occurs, delete BigQuery testing dataset before exiting with status code 1
    bq --project_id "${PROJECT_ID}" rm -r -f --dataset "${dataset_id}"
    printf "FAILURE: Encountered an error when running UDF tests for dataset: %s\n\n" "${dataset_id}"
    # TODO: Remove below comment before deploying
    # exit 1
  fi
  # Restore test_cases.js once UDFs are deployed
  mv "${udfs_target_dir}"/definitions/"${dataset_id}"/test_cases.js.ignore \
     "${udfs_target_dir}"/definitions/"${dataset_id}"/test_cases.js
}

test_udfs(){
  local project_id=$1
  local dataset_id=$2
  local udfs_source_dir=$3
  local udfs_target_dir=$4
  mkdir -p "${udfs_target_dir}"/definitions
  cp "${udfs_source_dir}"/test_cases.js "${udfs_target_dir}"/definitions/test_cases.js
  generate_dataform_config_and_creds "${project_id}" "${dataset_id}" "${udfs_target_dir}"
  add_symbolic_dataform_dependencies "${udfs_target_dir}"
  dataform test "${udfs_target_dir}"
}

main() {
  # For now, this build script assumes all BigQuery environments
  # live in the same location which you specify below.
  export BQ_LOCATION=US

  # Uncomment and set local variables below if testing locally.
  # These variables will come from cloud build env
   local PROJECT_ID=danny-bq
   local SHORT_SHA=hello

  if [[ -z "${PROJECT_ID}" ]]; then
    printf "You must set environment variable PROJECT_ID.\n"
    exit 1
  elif [[ -z "${JS_BUCKET}" ]]; then
    printf "No value set for environment variable JS_BUCKET.\n"
    export JS_BUCKET=gs://bqutil-lib/bq_js_libs # bucket used by bqutil project
    printf "Defaulting JS_BUCKET to %s\n" ${JS_BUCKET}
  fi

  # Create an empty dataform.json file because Dataform requires
  # this file's existence when installing dependencies.
  touch dataform.json
  # Only run 'dataform install' once, symbolic links will be used
  # for all other Dataform project directories.
  dataform install > /dev/null 2>&1

  local datasets
    cat ../../dir_to_dataset_map.yaml
    # Get the list of directory names which contain UDFs
    datasets=$(sed 's/:.*//g' < ../../dir_to_dataset_map.yaml)

  for dataset_id in ${datasets}; do
    # Get the short-hand version of the dataset_id
    # which is mapped in dir_to_dataset_map.yaml
    local short_dataset_id
    short_dataset_id=$(sed -rn "s/${dataset_id}: (.*)/\1/p" < ../../dir_to_dataset_map.yaml)
    printf "Testing UDFs in BigQuery dataset: %s%s\n" "${short_dataset_id}" "${SHORT_SHA}"

    # SHORT_SHA environment variable below comes from
    # cloud build when the trigger originates from a github commit.
    if [[ $dataset_id == 'community' ]]; then
      # Deploy all UDFs in the community folder
      deploy_udfs \
        "${PROJECT_ID}" \
        "${JS_BUCKET}" \
        "${dataset_id}" \
        "${short_dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../"${dataset_id}" \
        "${dataset_id}"_deploy
      # Run unit tests for all UDFs in community folder
      test_udfs \
        "${PROJECT_ID}" \
        "${short_dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../community \
        "${dataset_id}"_test
    else
      # Deploy all UDFs in the migration folder
      deploy_udfs \
        "${PROJECT_ID}" \
        "${JS_BUCKET}" \
        "${dataset_id}" \
        "${short_dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../migration/"${dataset_id}" \
        "${dataset_id}"_deploy
      # Run unit tests for all UDFs in migration folder
      test_udfs \
        "${PROJECT_ID}" \
        "${short_dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../migration/"${dataset_id}" \
        "${dataset_id}"_test
    fi

    # Remove testing directories to keep consecutive local runs clean
    rm -rf "${dataset_id}"_deploy
    rm -rf "${dataset_id}"_test
  done
}

clean # Uncomment only when testing locally
main
clean # Uncomment only when testing locally