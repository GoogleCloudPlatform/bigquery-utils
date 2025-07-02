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
# Create the following Dataform configuration and credentials
# files:
#   - dataform.json
#   - .df-credentials.json
#
# The dataform.json config file is created by replacing
# variables in dataform_template.json and copying
# the output into a file named dataform.json.
# The .df-credentials.json is created without any key specified
# so that ADC (application default credentials) are used to
# authenticate with BigQuery.
#
# Arguments:
#   project_id - BQ project in which dataform assets will be created
#   dataset_id - BQ dataset in which dataform assets will be created
#   gcs_bucket - GCS bucket which holds JavaScript UDF libraries or other assets
#   test_data_gcs_bucket - GCS bucket used for reading/writing test data during unit testing 
# Returns:
#   None
#######################################
generate_dataform_config_and_creds() {
  local project_id=$1
  local dataset_id=$2
  local gcs_bucket=$3
  local test_data_gcs_bucket=$4
  # Generate the dataform.json with the appropriate project_id and dataset_id
  sed "s|\${PROJECT_ID}|${project_id}|g" dataform_template.json \
    | sed "s|\${BQ_LOCATION}|${BQ_LOCATION}|g" \
    | sed "s|\${DATASET_ID}|${dataset_id}|g" \
    | sed "s|\${GCS_BUCKET}|${gcs_bucket}|g" \
    | sed "s|\${TEST_DATA_GCS_BUCKET}|${test_data_gcs_bucket}|g" \
    >dataform.json
  # Create an .df-credentials.json file as shown below
  # in order to have Dataform pick up application default credentials
  # https://cloud.google.com/docs/authentication/production#automatically
  echo "{\"projectId\": \"${project_id}\", \"location\": \"${BQ_LOCATION}\"}" >.df-credentials.json
}

#######################################
# Create the UDFs specified in the udfs_source_dir
# input argument.
# The following steps are taken:
#   - UDFs are copied into the Dataform definitions/ directory
#   - Dataform config/creds files (dataform.json and .df-credentials.json) are created
#   - 'dataform run' command is executed to deploy UDFs
# Arguments:
#   project_id ($1) - BQ project in which Dataform assets will be created.
#   dataset_id ($2) - BQ dataset where UDFs will be deployed. This name might include
#                     suffixes (e.g., from SHORT_SHA or region).
#   udfs_source_dir ($3) - Directory which holds UDF source files (.sqlx) to be deployed.
#   js_bucket ($4) - GCS bucket which holds JavaScript UDF libraries.
#######################################
deploy_udfs() {
  local project_id=$1
  local dataset_id=$2
  local udfs_source_dir=$3
  local js_bucket=$4
  local test_data_gcs_bucket=$5
  # Clear the definitions directory if it exists from a previous run
  rm -rf definitions
  # Create the Dataform definitions/ directory for the SQLX UDFs to be deployed
  mkdir definitions
  # Copy all UDF sources into the dataform definitions directory
  cp -RL "${udfs_source_dir}"/* definitions/
  # Remove test_cases.js file if it exists in the definitions directory
  # because this file is only used for testing UDFs
  rm -f definitions/test_cases.js
  generate_dataform_config_and_creds "${project_id}" "${dataset_id}" "${js_bucket}" "${test_data_gcs_bucket}"

  ls -la
  ls -la definitions

  printf "Deploying UDFs using dataform run command\n"
  if ! (dataform run . --timeout=10m); then
    # If any error occurs, delete BigQuery testing dataset before exiting with status code 1
    # If SHORT_SHA is not null, then we know a test dataset was used.
    if [[ -n "${SHORT_SHA}" ]]; then
      bq --project_id "${project_id}" rm -r -f --dataset "${dataset_id}"
    fi
    printf "FAILURE: Encountered an error when deploying UDFs in dataset: %s\n\n" "${dataset_id}"
    exit 1
  fi
}

#######################################
# Execute all unit tests provided in a test_cases.js file.
# The following steps are taken:
#   - test_cases.js file is copied from the specified udfs_source_dir into definitions/ directory
#   - "dataform test" command is executed to test all UDFs
#   - Any error in testing will terminate with the deletion of the BigQuery testing dataset
# Arguments:
#   project_id - BQ project in which UDFs to be unit tested exist
#   dataset_id - BQ dataset in which UDFs to be unit tested exist
#   udfs_source_dir - Directory which holds the test_cases.js file to be run
#######################################
test_udfs() {
  local project_id=$1
  local dataset_id=$2
  local udfs_source_dir=$3
  # Clear the definitions directory if it exists from a previous run
  rm -rf definitions
  # Create the Dataform definitions/ directory for only the test_cases.js to be run
  mkdir definitions
  # Only run Dataform tests if a test_cases.js exists
  if [[ -f "${udfs_source_dir}"/test_cases.js ]]; then
    cp "${udfs_source_dir}"/test_cases.js definitions/test_cases.js
    printf "Testing UDFs using dataform test command\n"
    if ! (dataform test .); then
      # If any error occurs when testing, delete BigQuery testing dataset before exiting with status code 1.
      # If SHORT_SHA is not null, then we know a test dataset was used.
      if [[ -n "${SHORT_SHA}" ]]; then
        bq --project_id "${project_id}" rm -r -f --dataset "${dataset_id}"
      fi
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
    printf "Defaulting BQ_LOCATION to %s\n" "${BQ_LOCATION}"
  fi
  if [[ -z "${JS_BUCKET}" ]]; then
    printf "No value set for environment variable JS_BUCKET.\n"
    export JS_BUCKET=gs://bqutil-lib/bq_js_libs # bucket used by bqutil project
    printf "Defaulting JS_BUCKET to %s\n" "${JS_BUCKET}"
  fi
  if [[ -z "${TEST_DATA_GCS_BUCKET}" ]]; then
    printf "No value set for environment variable TEST_DATA_GCS_BUCKET.\n"
    export TEST_DATA_GCS_BUCKET="${JS_BUCKET}"
    printf "Defaulting TEST_DATA_GCS_BUCKET to %s\n" "${JS_BUCKET}"
  fi

  # Create an empty dataform.json file because Dataform requires
  # this file's existence when installing dependencies.
  touch dataform.json
  # Only run 'dataform install' once, symbolic links will be used
  # for all other Dataform project directories.
  dataform install >/dev/null 2>&1

  # Deploy UDFs to bigquery-public-data project
  public_dataset_id="persistent_udfs"
  if [[ "${PROJECT_ID}" = "bigquery-public-data" ]]; then
      # Deploy all UDFs in the community folder
      deploy_udfs \
        "${PROJECT_ID}" \
        "${public_dataset_id}" \
        "$(pwd)"/../../community \
        "${JS_BUCKET}" \
        "${TEST_DATA_GCS_BUCKET}"
      # Copy test_data used by some unit tests
      gcloud storage cp -r "$(pwd)"/../test_data/* "${TEST_DATA_GCS_BUCKET}/test_data/"
      # Run unit tests for all UDFs in community folder
      test_udfs \
        "${PROJECT_ID}" \
        "${public_dataset_id}" \
        "$(pwd)"/../../community
  else
    local udf_dirs
    # Get the list of directory names which contain UDFs
    udf_dirs=$(sed 's/:.*//g' <../../dir_to_dataset_map.yaml)

    for udf_dir in ${udf_dirs}; do
      # Get the short-hand version of the dataset_id
      # which is mapped in dir_to_dataset_map.yaml
      local dataset_id
      dataset_id=$(sed -rn "s/${udf_dir}: (.*)/\1/p" <../../dir_to_dataset_map.yaml)
      # Region suffixes are used to deploy UDFs globally to bqutil without naming conflicts
      # US multi-region datasets remain without suffix to avoid breaking changes to legacy users
      if [[  "${BQ_LOCATION^^}" != "US" ]]; then
        local region_suffix=$(echo "$BQ_LOCATION" | tr '[:upper:]' '[:lower:]' | tr '-' '_')
        dataset_id="${dataset_id}_${region_suffix}"
        printf "Dataset ID with region suffix: %s\n" "${dataset_id}"
      fi
      printf "*************** "
      printf "Testing UDFs in BigQuery dataset: %s%s" "${dataset_id}" "${SHORT_SHA}"
      printf " ***************\n"

      # SHORT_SHA environment variable below comes from
      # cloud build when the trigger originates from a github commit.
      if [[ $udf_dir == 'community' ]]; then
        # Deploy all UDFs in the community folder
        deploy_udfs \
          "${PROJECT_ID}" \
          "${dataset_id}${SHORT_SHA}" \
          "$(pwd)"/../../"${udf_dir}" \
          "${JS_BUCKET}" \
          "${TEST_DATA_GCS_BUCKET}"
        # Copy test_data used by some unit tests
        gcloud storage cp -r "$(pwd)"/../test_data/* "${TEST_DATA_GCS_BUCKET}/test_data/"
        # Run unit tests for all UDFs in community folder
        test_udfs \
          "${PROJECT_ID}" \
          "${dataset_id}${SHORT_SHA}" \
          "$(pwd)"/../../"${udf_dir}"
      else # Deploy all UDFs in the migration folder
        deploy_udfs \
          "${PROJECT_ID}" \
          "${dataset_id}${SHORT_SHA}" \
          "$(pwd)"/../../migration/"${udf_dir}" \
          "${JS_BUCKET}" \
          "${TEST_DATA_GCS_BUCKET}"
        # Run unit tests for all UDFs in migration folder
        test_udfs \
          "${PROJECT_ID}" \
          "${dataset_id}${SHORT_SHA}" \
          "$(pwd)"/../../migration/"${udf_dir}"
      fi

      printf "Finished testing UDFs in BigQuery dataset: %s%s\n" "${dataset_id}" "${SHORT_SHA}"

      if [[ -n "${SHORT_SHA}" ]]; then
        printf "Deleting BigQuery dataset %s because setting env var SHORT_SHA=%s means this is a test build\n" "${dataset_id}${SHORT_SHA}" "${SHORT_SHA}"
        bq --project_id "${PROJECT_ID}" rm -r -f --dataset "${dataset_id}${SHORT_SHA}"
      fi
    done
  fi
}

# export PROJECT_ID= # Uncomment and set if testing locally
main
