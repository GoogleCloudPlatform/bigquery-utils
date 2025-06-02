#!/usr/bin/env bash

# Copyright 2025 Google LLC
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

# Remove testing assets
# Globals:
#   _BQ_DATASET
#   _JS_BUCKET
# Arguments:
#   None
# Returns:
#   None
#######################################
function remove_testing_assets(){
  printf "Deleting BigQuery dataset: %s\n" "${_BQ_DATASET}"
  bq --headless --synchronous_mode rm -r -f "${_BQ_DATASET}"
  printf "Deleting Cloud Storage directory: %s\n" "${_JS_BUCKET}"
  gcloud storage rm -r "${_JS_BUCKET}/**"
}

##############################################
# Deploys datasketch UDFs and runs unit tests.
# Globals:
#   PROJECT_ID
#   _BQ_DATASET
#   _JS_BUCKET
#   _BQ_LOCATION
# Arguments:
#   None
# Returns:
#   None
##############################################
function deploy_udfs_and_run_unit_tests() {
  set -eo pipefail
  git clone --single-branch --branch $(cat VERSION.txt) https://github.com/apache/datasketches-bigquery.git
  cd datasketches-bigquery

  if [[ "${_BQ_LOCATION^^}" != "US" ]]; then
    printf "Deploying to regional BigQuery location: %s\n" "${_BQ_LOCATION}"
    export _BQ_DATASET="${_BQ_DATASET}_$(echo $_BQ_LOCATION | tr '[:upper:]' '[:lower:]' | tr '-' '_')"
    printf "BigQuery regional dataset name: %s\n" "${_BQ_DATASET}"
  fi

  # TODO: Remove this temporary workaround once datasketches repo releases this fix --> https://github.com/apache/datasketches-bigquery/pull/156 
  sed -i 's|git clone https://github.com/emscripten-core/emsdk.git|git clone --branch 4.0.7 --single-branch https://github.com/emscripten-core/emsdk.git|g' cloudbuild.yaml

  gcloud builds submit . \
    --project=$PROJECT_ID \
    --region="us-central1" \
    --worker-pool="projects/${PROJECT_ID}/locations/us-central1/workerPools/udf-unit-testing" \
    --polling-interval="10" \
    --substitutions=_BQ_LOCATION=$_BQ_LOCATION,_BQ_DATASET=$_BQ_DATASET,_JS_BUCKET=$_JS_BUCKET
}

#######################################
# Main entry-point for execution
# Globals:
#   BRANCH_NAME
#   SHORT_SHA
#   _PR_NUMBER
#   _BQ_DATASET
#   _JS_BUCKET
# Arguments:
#   None
# Returns:
#   None
#######################################
function main() {
  # Only deploy UDFs when building master branch and there is
  # no associated pull request, meaning the PR was approved
  # and this is now building a commit on master branch.
  if [[ "${BRANCH_NAME}" == "master" && -z "${_PR_NUMBER}" ]]; then
    deploy_udfs_and_run_unit_tests
  else
    # Add SHORTSHA to _JS_BUCKET and _BQ_DATASET to prevent
    # collisions between concurrent builds.
    export _JS_BUCKET="${_JS_BUCKET}/${SHORT_SHA}"
    export _BQ_DATASET="${_BQ_DATASET}_${SHORT_SHA}"
    if ! deploy_udfs_and_run_unit_tests ; then
      printf "FAILURE: Build process for datasketch UDFs failed, running cleanup steps:\n"
      remove_testing_assets
      exit 1
    fi
    remove_testing_assets
  fi
}

main