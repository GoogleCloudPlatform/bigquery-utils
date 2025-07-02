#!/usr/bin/env bash

# Copyright 2019 Google LLC
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
set -eo pipefail


# Directory of the UDFs
UDF_DIR=udfs
# Directory of the Stored Procedures
SP_DIR=stored_procedures

export TERM=xterm-256color
# Set colors if terminal supports it
ncolors=$(tput colors)
if [[ "${ncolors}" -gt 7 ]]; then
  BOLD=$(tput bold)
  NORMAL=$(tput sgr0)
fi

#######################################
# Executes a query file. If the execution
# fails, the script will exit with an error
# code of 1.
# Globals:
#   BOLD
#   NORMAL
#   LOCATION
# Arguments:
#   file
#   dry_run
# Returns:
#   None
#######################################
function execute_query() {
  local file=$1
  local dry_run=$2

  printf "%s%s%s\n" "${BOLD}" "${file}" "${NORMAL}"
  if [[ ${dry_run} == true ]]; then
    if ! bq query \
    --headless --nouse_legacy_sql --dry_run < "${file}" ; then
      printf "Failed to dry run: %s" "${file}"
      # exit 1 is not called here because some dry-runs may fail due to
      # variable placeholders which a user must replace with their own values.
      # These dry-runs require manual revision of results.
    fi
  else
    if ! bq query \
    --headless --nouse_legacy_sql < "${file}" ; then
      printf "Failed to create: %s" "${file}"
      exit 1
    fi
  fi
}

#######################################
# Builds and hosts the Cloud Build image
# used to test BigQuery UDFs.
# Globals:
#   UDF_DIR
# Arguments:
#   None
# Returns:
#   None
#######################################
function build_udf_testing_image() {
  gcloud builds submit "${UDF_DIR}"/tests/ \
    --config="${UDF_DIR}"/tests/cloudbuild_udf_test_image.yaml
}

# Remove the ${_JS_BUCKET} directory
# Globals:
#   _JS_BUCKET
# Arguments:
#   None
# Returns:
#   None
#######################################
function remove_gcs_js_directory(){
  printf "Deleting Cloud Storage directory: %s\n" "${_JS_BUCKET}"
  gcloud storage rm -r "${_JS_BUCKET}/**"
}

# Remove test data from ${_TEST_DATA_GCS_BUCKET}
# Globals:
#   _TEST_DATA_GCS_BUCKET
# Arguments:
#   None
# Returns:
#   None
#######################################
function remove_gcs_test_data(){
  printf "Deleting Cloud Storage directory: %s\n" "${_TEST_DATA_GCS_BUCKET}"
  gcloud storage rm -r "${_TEST_DATA_GCS_BUCKET}/**"
}

#######################################
# Builds all BigQuery UDFs within the repository.
# Globals:
#   UDF_DIR
#   _JS_BUCKET
#   SHORT_SHA
#   _BQ_LOCATION
# Arguments:
#   None
# Returns:
#   None
#######################################
function build_udfs() {
  # Create all UDFs in a test dataset unique to the commit SHORT_SHA.
  # Perform unit tests on any UDFs which have test cases.
  # Delete test datasets when finished
  if [[ -n "${SHORT_SHA}" ]]; then
    if ! gcloud builds submit "${UDF_DIR}"/ \
      --region="us-central1" \
      --config="${UDF_DIR}"/cloudbuild.yaml \
      --polling-interval="10" \
      --worker-pool="projects/${PROJECT_ID}/locations/us-central1/workerPools/udf-unit-testing" \
      --substitutions _JS_BUCKET="${_JS_BUCKET}",SHORT_SHA="${SHORT_SHA}",_BQ_LOCATION="${_BQ_LOCATION},_TEST_DATA_GCS_BUCKET=${_TEST_DATA_GCS_BUCKET}" ; then
      # Delete BigQuery UDF test datasets and cloud storage directory if above cloud build process fails
      printf "FAILURE: Build process for BigQuery UDFs failed, running cleanup steps:\n"
      local datasets
      datasets=$(sed 's/.*: //g' < udfs/dir_to_dataset_map.yaml)
      for dataset in ${datasets}; do
        if [[ "${_BQ_LOCATION^^}" != "US" ]]; then
          local region_suffix=$(echo "$_BQ_LOCATION" | tr '[:upper:]' '[:lower:]' | tr '-' '_')
          dataset="${dataset}_${region_suffix}"
        fi
        printf "Deleting BigQuery dataset: %s%s\n" "${dataset}" "${SHORT_SHA}"
        bq --headless --synchronous_mode rm -r -f "${dataset}${SHORT_SHA}"
      done
      remove_gcs_js_directory
      remove_gcs_test_data
      exit 1
    fi
    remove_gcs_js_directory
    remove_gcs_test_data
  fi
}

#######################################
# Executes dry-runs of all SQL scripts
# within the repository except for the
# udfs/ and the tools/ directories
# since they're tested separately.
# Globals:
#   _BQ_LOCATION
# Arguments:
#   None
# Returns:
#   None
#######################################
function dry_run_all_sql() {
  if [[ "${_BQ_LOCATION^^}" == "US" ]]; then
    # Get list of all .sql files
    # (excluding udfs/ and tools/ directories)
    local sql_files
    sql_files=$(find . \
    -wholename "./udfs/*" -prune -o \
    -wholename "./tools/*" -prune -o \
    -type f -name "*.sql" -print)
    local num_files
    num_files=$(echo "${sql_files}" | wc -l)

    printf "Dry-running %s SQL assets...\n" "${num_files}"
    while read -r file; do
      execute_query "${file}" true
    done <<<"${sql_files}"
  fi
}

#######################################
# Executes all build steps for SQL
# assets within the repository.
# Globals:
#   UDF_DIR
#   _BUILD_IMAGE (true/false)
# Arguments:
#   None
# Returns:
#   None
#######################################
function build() {
  # Get a list of changed files in this commit.
  local files_changed
  files_changed=$(git diff --name-only origin/master)
  # Only build the Cloud Build image (used for testing UDFs)
  # if any files in the udfs/tests/ directory have changed.
  if [[ $(echo "${files_changed}" | grep -q "${UDF_DIR}"/tests/Dockerfile.ci) || -n "${_BUILD_IMAGE}" ]]; then
    build_udf_testing_image
  fi
  # Only build the BigQuery UDFs if any files in the
  # udfs/ directory have been changed
  if echo "${files_changed}" | grep -q "${UDF_DIR}"/; then
    printf "Building BigQuery UDFs since the following files have changed:\n%s\n" "${files_changed}"
    build_udfs
  fi
  # Only build the BigQuery Stored Procedures if any files in the
  # stored_procedures/ directory have been changed
  if echo "${files_changed}" | grep -q "${SP_DIR}"/; then
    printf "Building BigQuery stored procedures since the following files have changed:\n%s\n" "${files_changed}"
    cd "${SP_DIR}" && ./deploy.sh
  fi
}

#######################################
# Deploys UDFs to their associated datasets
# within the bqutil project
# Globals:
#   UDF_DIR
#   _JS_BUCKET
#   _BQ_LOCATION
# Arguments:
#   None
# Returns:
#   None
#######################################
function deploy_udfs() {
  local sql_files
  sql_files=$(find "${UDF_DIR}" -type f -name "*.sql")
  local num_files
  num_files=$(echo "${sql_files}" | wc -l)

  # For prod deploys, do not set SHORT_SHA so that BQ dataset
  # names do not get the SHORT_SHA value added as a suffix.
  gcloud builds submit "${UDF_DIR}"/ \
    --region="us-central1" \
    --config="${UDF_DIR}"/cloudbuild.yaml \
    --polling-interval="10" \
    --worker-pool="projects/${PROJECT_ID}/locations/us-central1/workerPools/udf-unit-testing" \
    --substitutions SHORT_SHA=,_JS_BUCKET="${_JS_BUCKET}",_BQ_LOCATION="${_BQ_LOCATION},"_TEST_DATA_GCS_BUCKET="${_TEST_DATA_GCS_BUCKET}"
}

#######################################
# Main entry-point for execution
# Globals:
#   BRANCH_NAME
#   _PR_NUMBER
# Arguments:
#   None
# Returns:
#   None
#######################################
function main() {
  # Add SHORTSHA to _TEST_DATA_GCS_BUCKET env variable
  # to prevent collisions between concurrent builds
  export _TEST_DATA_GCS_BUCKET="${_TEST_DATA_GCS_BUCKET}/${SHORT_SHA}"
  # Only deploy UDFs when building master branch and there is
  # no associated pull request, meaning the PR was approved
  # and this is now building a commit on master branch.
  if [[ "${BRANCH_NAME}" == "master" && -z "${_PR_NUMBER}" ]]; then
    deploy_udfs
    # For prod deploys of stored procedures, do not set SHORT_SHA so that BQ dataset
    # names do not get the SHORT_SHA value added as a suffix.
    cd "${SP_DIR}" && export SHORT_SHA="" && ./deploy.sh
  else
    # Add SHORTSHA to _JS_BUCKET env variable 
    # to prevent collisions between concurrent builds
    export _JS_BUCKET="${_JS_BUCKET}/${SHORT_SHA}"
    build
    dry_run_all_sql
  fi
}

main
