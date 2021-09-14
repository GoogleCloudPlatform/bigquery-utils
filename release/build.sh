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

# Directory of the UDFs
UDF_DIR=udfs

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
  if [[ ${dry_run} = true ]]; then
    if ! bq query \
    --headless --nouse_legacy_sql --dry_run "$(cat "${file}")" ; then
      printf "Failed to dry run: %s" "${file}"
      # exit 1 is not called here because some dry-runs may fail due to
      # variable placeholders which a user must replace with their own values.
      # These dry-runs require manual revision of results.
    fi
  else
    if ! bq query \
    --headless --nouse_legacy_sql "$(cat "${file}")" ; then
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

#######################################
# Replaces all ${JS_BUCKET} placeholders
# in javascript UDFs with bigquery-utils
# public hosting bucket.
# Globals:
#   UDF_DIR
#   _JS_BUCKET
# Arguments:
#   None
# Returns:
#   None
#######################################
function replace_js_udf_bucket_placeholder() {
  # Replace all variable placeholders "${JS_BUCKET}" in Javascript UDFs
  # with the bucket that will host all javascript libraries
  local sql_files
  sql_files=$(find ${UDF_DIR} -type f -name "*.sql")
  local num_files
  num_files=$(echo "${sql_files}" | wc -l)
  printf "Replacing UDF bucket placeholder \${JS_BUCKET} with %s\n" "${_JS_BUCKET}"
  while read -r file; do
    sed -i "s|\${JS_BUCKET}|${_JS_BUCKET}|g" "${file}"
  done <<<"${sql_files}"
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
  gsutil rm -rf "${_JS_BUCKET}"
}

#######################################
# Builds all BigQuery UDFs within the repository.
# Globals:
#   UDF_DIR
#   _JS_BUCKET
#   SHORT_SHA
# Arguments:
#   None
# Returns:
#   None
#######################################
function build_udfs() {
  # Create all UDFs in a test dataset unique to the commit SHORT_SHA.
  # Perform unit tests on any UDFs which have test cases.
  # Delete test datasets when finished
  if ! gcloud builds submit "${UDF_DIR}"/ \
    --config="${UDF_DIR}"/cloudbuild.yaml \
    --substitutions _JS_BUCKET="${_JS_BUCKET}",SHORT_SHA="${SHORT_SHA}",_BQ_LOCATION="${_BQ_LOCATION}" ; then
    # Delete BigQuery UDF test datasets and cloud storage directory if above cloud build process fails
    printf "FAILURE: Build process for BigQuery UDFs failed, running cleanup steps:\n"
    local datasets
    datasets=$(sed 's/.*: //g' < udfs/dir_to_dataset_map.yaml)
    for dataset in ${datasets}; do
      printf "Deleting BigQuery dataset: %s_test_%s\n" "${dataset}" "${SHORT_SHA}"
      bq --headless --synchronous_mode rm -r -f "${dataset}_test_${SHORT_SHA}"
    done
    remove_gcs_js_directory
    exit 1
  fi
  remove_gcs_js_directory
}

#######################################
# Executes dry-runs of all SQL scripts
# within the repository except for the
# udfs/ and the tools/ directories
# since they're tested separately.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
#######################################
function dry_run_all_sql() {
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
}

#######################################
# Executes all build steps for SQL
# assets within the repository.
# Globals:
#   UDF_DIR
# Arguments:
#   None
# Returns:
#   None
#######################################
function build() {
  replace_js_udf_bucket_placeholder

  # Get a list of changed files in this commit.
  local files_changed
  files_changed=$(git diff --name-only origin/master)

  # Only build the Cloud Build image (used for testing UDFs)
  # if any files in the udfs/tests/ directory have changed.
  if echo "${files_changed}" | grep -q "${UDF_DIR}"/tests/Dockerfile.ci; then
    build_udf_testing_image
  fi

  # Only build the BigQuery UDFs if any files in the
  # udfs/ directory have been changed
  if echo "${files_changed}" | grep -q "${UDF_DIR}"/; then
    build_udfs
  fi

}

#######################################
# Deploys UDFs to their associated datasets
# within the bqutil project
# Globals:
#   UDF_DIR
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

  replace_js_udf_bucket_placeholder

  # For prod deploys, do not set SHORT_SHA so that BQ dataset
  # names do not get the SHORT_SHA value added as a suffix.
  gcloud builds submit "${UDF_DIR}"/ \
    --config="${UDF_DIR}"/cloudbuild.yaml \
    --substitutions SHORT_SHA=,_JS_BUCKET="${_JS_BUCKET}",_BQ_LOCATION="${_BQ_LOCATION}"
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
  # Only deploy UDFs when building master branch and there is
  # no associated pull request, meaning the PR was approved
  # and this is now building a commit on master branch.
  if [[ "${BRANCH_NAME}" = "master" && -z "${_PR_NUMBER}" ]]; then
    deploy_udfs
  else
    build
    dry_run_all_sql
  fi
}

main
