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
    if [[ -n $file ]]; then
      printf "Replacing variables in file %s\n" "$file"
      sed -i "s|\${JS_BUCKET}|${js_bucket}|g" "${file}"
    else
      printf "No SQLX files found in %s\n" "${udf_dir}"
    fi
  done <<< "$(find "${udf_dir}" -type f -name "*.sqlx")"
}

#######################################
# Create symbolic links to the following dataform dependencies
# to save time and not duplicate resources:
#   - package.json file
#   - includes/ dir
#   - node_modules/ dir
#
# Arguments:
#   target_dir - Directory where symbolic links are created
# Returns:
#   None
#######################################
add_symbolic_dataform_dependencies() {
  local target_dir=$1
  ln -sf "$(pwd)"/package.json "${target_dir}"/package.json
  ln -sfn "$(pwd)"/node_modules/ "${target_dir}"/node_modules
  ln -sfn "$(pwd)"/includes/ "${target_dir}"/includes
}

#######################################
# Create the following Dataform configuration and credentials
# files:
#   - dataform.json
#   - .df-credentials.json
#
# The dataform.json config file is created by replacing
# variables in dataform_template.json and copying
# the output into a file named dataform.json and placed
# within the specified target_dir input argument.
# The .df-credentials.json is created without any key specified
# so that ADC (application default credentials) are used to
# authenticate with BigQuery.
#
# Arguments:
#   project_id - BQ project in which dataform assets will be created
#   dataset_id - BQ dataset in which dataform assets will be created
#   target_dir - Directory in which dataform.json file is created
# Returns:
#   None
#######################################
generate_dataform_config_and_creds() {
  local project_id=$1
  local dataset_id=$2
  local target_dir=$3
  # Generate the dataform.json with the appropriate project_id and dataset_id
  sed "s|\${PROJECT_ID}|${project_id}|g" dataform_template.json \
    | sed "s|\${DATASET_ID}|${dataset_id}|g" >"${target_dir}"/dataform.json
  # Create an .df-credentials.json file as shown below
  # in order to have Dataform pick up application default credentials
  # https://cloud.google.com/docs/authentication/production#automatically
  echo "{\"projectId\": \"${project_id}\", \"location\": \"${BQ_LOCATION}\"}" >"${target_dir}"/.df-credentials.json
}

#######################################
# Create the UDFs specified in the udfs_source_dir
# input argument.
# The following steps are taken:
#   - UDFs are copied into a new directory.
#   - test_cases.js file is temporarily renamed so that Dataform doesn't try to run it.
#   - JS_BUCKET variable placeholder in all .sqlx files is replaced with the env variable value
#   - Dataform config/creds files (dataform.json and .df-credentials.json) are created
#   - Symbolic links to Dataform dependencies are created
#   - 'dataform run' command is executed to deploy UDFs
#   - test_cases.js file is restored to its original name
# Arguments:
#   project_id - BQ project in which dataform assets will be created
#   js_bucket - GCS bucket which holds JavaScript UDF libraries
#   dataset_id - Name of dataset directory which holds .sqlx UDFs to be deployed
#   short_dataset_id - BQ dataset in which dataform assets will be deployed
#   udfs_source_dir - Directory which holds UDFs to be deployed
#   udfs_target_dir - Temp directory in which UDFs will be copied for deployment
#######################################
deploy_udfs() {
  local project_id=$1
  local js_bucket=$2
  local udf_dir=$3
  local dataset_id=$4
  local udfs_source_dir=$5
  local udfs_target_dir=$6
  mkdir -p "${udfs_target_dir}"/definitions
  # Copy all UDF sources into the target dir to avoid modifying the source itself.
  # Option -L is used to copy actual files to which symlinks point.
  cp -RL "${udfs_source_dir}"/ "${udfs_target_dir}"/definitions/"${udf_dir}"

  # Remove test_cases.js avoid deploying this file
  rm -f "${udfs_target_dir}"/definitions/"${udf_dir}"/test_cases.js

  replace_js_udf_bucket_placeholder "${udfs_target_dir}"/definitions/"${udf_dir}" "${js_bucket}"
  generate_dataform_config_and_creds "${project_id}" "${dataset_id}" "${udfs_target_dir}"
  add_symbolic_dataform_dependencies "${udfs_target_dir}"

  printf "Deploying UDFs from %s using dataform run command.\n" "${udfs_source_dir}"
  if ! dataform run "${udfs_target_dir}"; then
    # If any error occurs, delete BigQuery testing dataset before exiting with status code 1
    # If SHORT_SHA is not null, then we know a test dataset was used.
    if [[ -n "${SHORT_SHA}" ]]; then
      bq --project_id "${project_id}" rm -r -f --dataset "${dataset_id}"
    fi
    printf "FAILURE: Encountered an error when deploying UDFs in dataset: %s\n\n" "${udf_dir}"
    exit 1
  fi
}

#######################################
# Execute all unit tests provided in a test_cases.js file.
# The following steps are taken:
#   - test_cases.js file is copied from the specified udfs_source_dir into udfs_target_dir
#   - Dataform config/creds files (dataform.json and .df-credentials.json) are created
#   - Symbolic links to Dataform dependencies are created
#   - "dataform test" command is executed to test all UDFs
#   - Any error in testing will terminate with the deletion of the BigQuery testing dataset
# Arguments:
#   project_id - BQ project in which UDFs to be unit tested exist
#   dataset_id - BQ dataset in which UDFs to be unit tested exist
#   udfs_source_dir - Directory which holds the test_cases.js file to be run
#   udfs_target_dir - Temp directory in which test_cases.js will be copied and then run
#######################################
test_udfs() {
  local project_id=$1
  local dataset_id=$2
  local udfs_source_dir=$3
  local udfs_target_dir=$4
  mkdir -p "${udfs_target_dir}"/definitions
  # Only run Dataform tests if a test_cases.js exists
  if [[ -f "${udfs_source_dir}"/test_cases.js ]]; then
    cp "${udfs_source_dir}"/test_cases.js "${udfs_target_dir}"/definitions/test_cases.js
    generate_dataform_config_and_creds "${project_id}" "${dataset_id}" "${udfs_target_dir}"
    add_symbolic_dataform_dependencies "${udfs_target_dir}"
    if ! dataform test "${udfs_target_dir}"; then
      # If any error occurs when testing, delete BigQuery testing dataset before exiting with status code 1.
      # If SHORT_SHA is not null, then we know a test dataset was used.
      if [[ -n "${SHORT_SHA}" ]]; then
        bq --project_id "${project_id}" rm -r -f --dataset "${dataset_id}"
      fi
      rm -rf "${dataset_id}"_test
      printf "FAILURE: Encountered an error when running UDF tests for dataset: %s\n\n" "${dataset_id}"
      exit 1
    fi
  else
    printf "Skipping Dataform unit tests since no test_cases.js file found.\n"
  fi
}

#######################################
# This is the main entry point of this bash script.
# This function orchestrates the entire UDF unit testing
# process.
# Globals:
#   BQ_LOCATION
#   JS_BUCKET
#   PROJECT_ID
#   SHORT_SHA
# Arguments:
#  None
#######################################
main() {

  if [[ -z "${PROJECT_ID}" ]]; then
    printf "You must set environment variable PROJECT_ID.\n"
    exit 1
  fi
  if [[ -z "${BQ_LOCATION}" ]]; then
    printf "No value set for environment variable BQ_LOCATION.\n"
    export BQ_LOCATION=US
    printf "Defaulting BQ_LOCATION to %s\n" ${BQ_LOCATION}
  fi
  if [[ -z "${JS_BUCKET}" ]]; then
    printf "No value set for environment variable JS_BUCKET.\n"
    export JS_BUCKET=gs://bqutil-lib/bq_js_libs # bucket used by bqutil project
    printf "Defaulting JS_BUCKET to %s\n" ${JS_BUCKET}
  fi

  # Create an empty dataform.json file because Dataform requires
  # this file's existence when installing dependencies.
  touch dataform.json
  # Only run 'dataform install' once, symbolic links will be used
  # for all other Dataform project directories.
  dataform install >/dev/null 2>&1

  local udf_dirs
  # Get the list of directory names which contain UDFs
  udf_dirs=$(sed 's/:.*//g' <../../dir_to_dataset_map.yaml)

  for udf_dir in ${udf_dirs}; do
    # Get the short-hand version of the dataset_id
    # which is mapped in dir_to_dataset_map.yaml
    local dataset_id
    dataset_id=$(sed -rn "s/${udf_dir}: (.*)/\1/p" <../../dir_to_dataset_map.yaml)
    printf "*************** "
    printf "Testing UDFs in BigQuery dataset: %s%s" "${dataset_id}" "${SHORT_SHA}"
    printf " ***************\n"

    # SHORT_SHA environment variable below comes from
    # cloud build when the trigger originates from a github commit.
    if [[ $udf_dir == 'community' ]]; then
      # Deploy all UDFs in the community folder
      deploy_udfs \
        "${PROJECT_ID}" \
        "${JS_BUCKET}" \
        "${udf_dir}" \
        "${dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../"${udf_dir}" \
        "${udf_dir}"_deploy
      # Run unit tests for all UDFs in community folder
      test_udfs \
        "${PROJECT_ID}" \
        "${dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../community \
        "${udf_dir}"_test
    else # Deploy all UDFs in the migration folder
      deploy_udfs \
        "${PROJECT_ID}" \
        "${JS_BUCKET}" \
        "${udf_dir}" \
        "${dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../migration/"${udf_dir}" \
        "${udf_dir}"_deploy
      # Run unit tests for all UDFs in migration folder
      test_udfs \
        "${PROJECT_ID}" \
        "${dataset_id}${SHORT_SHA}" \
        "$(pwd)"/../../migration/"${udf_dir}" \
        "${udf_dir}"_test
    fi

    printf "Finished testing UDFs in BigQuery dataset: %s%s\n" "${dataset_id}" "${SHORT_SHA}"
    # Remove testing directories to keep consecutive local runs clean
    rm -rf "${udf_dir}"_deploy
    rm -rf "${udf_dir}"_test
    printf "Finished cleaning temp directories %s and %s\n" "${udf_dir}"_deploy "${udf_dir}"_test

    if [[ -n "${SHORT_SHA}" ]]; then
      printf "Deleting BigQuery dataset %s because setting env var SHORT_SHA=%s means this is a test build\n" "${dataset_id}${SHORT_SHA}" "${SHORT_SHA}"
      bq --project_id "${PROJECT_ID}" rm -r -f --dataset "${dataset_id}${SHORT_SHA}"
    fi
  done
}

# export PROJECT_ID= # Uncomment and set if testing locally
main
