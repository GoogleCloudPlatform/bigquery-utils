#!/usr/bin/env bash

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

##############################################
# Build UDF testing image if it does not exist.
# Globals:
#   PROJECT_ID
#   JS_BUCKET
# Arguments:
#   None
# Returns:
#   None
##############################################
build_image_if_not_exists() {
  if ! gcloud container images describe "gcr.io/${PROJECT_ID}/bq_udf_ci:infrastructure-public-image-bqutil" 2> /dev/null; then
    printf "Build image does not exist. Building image %s.\n" "gcr.io/${PROJECT_ID}/bq_udf_ci:infrastructure-public-image-bqutil"
    gcloud builds submit tests/ --config=tests/cloudbuild_udf_test_image.yaml
  fi
}

if [[ -n "${PROJECT_ID}" ]]; then
  build_image_if_not_exists
elif [[ -n $(gcloud config get-value project) ]]; then
  export PROJECT_ID=$(gcloud config get-value project)
  printf "Env variable PROJECT_ID is not set. Retrieving default value from current gcloud configuration.\n"
  printf "Running with PROJECT_ID=%s.\n" "${PROJECT_ID}"
  build_image_if_not_exists
else
  printf "Set env variable PROJECT_ID to your own project id.\n"
  printf "For example, run the following to set PROJECT_ID:\n export PROJECT_ID=YOUR_PROJ_ID\n"
  exit 1
fi

if [[ -n "${JS_BUCKET}" ]]; then
  gcloud storage cp -r tests/test_data/* ${JS_BUCKET}/test_data/
  gcloud builds submit . \
    --project="${PROJECT_ID}" \
    --substitutions _BQ_LOCATION="${BQ_LOCATION}",SHORT_SHA=_test_env,_JS_BUCKET="${JS_BUCKET},_TEST_DATA_GCS_BUCKET=${JS_BUCKET}"
else
  printf "Set env variable JS_BUCKET to your own GCS bucket where Javascript libraries can be deployed.\n"
  printf "For example, run the following to set JS_BUCKET:\n export JS_BUCKET=gs://YOUR_BUCKET/PATH/TO/LIBS\n"
fi



