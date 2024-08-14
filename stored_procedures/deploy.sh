#!/bin/bash

# Copyright 2024 Google LLC
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


DATASET_ID="procedure"
# _BQ_LOCATION is an environment variable that is set in the Cloud Build trigger
BQ_LOCATION=$_BQ_LOCATION

if [[ -z "${PROJECT_ID}" ]]; then
  printf "You must set environment variable PROJECT_ID.\n"
  exit 1
fi
if [[ -z "${BQ_LOCATION}" ]]; then
  printf "No value set for environment variable BQ_LOCATION.\n"
  export BQ_LOCATION=US
  printf "Defaulting BQ_LOCATION to %s.\n" ${BQ_LOCATION}
fi

echo "{\"projectId\": \"${PROJECT_ID}\", \"location\": \"${BQ_LOCATION}\"}" >.df-credentials.json

# Install NPM dependencies.
dataform install >/dev/null 2>&1

if [[  "${BQ_LOCATION^^}" != "US" ]]; then
  REGION_SUFFIX=$(echo "$BQ_LOCATION" | tr '[:upper:]' '[:lower:]' | tr '-' '_')
  DATASET_ID="${DATASET_ID}_${REGION_SUFFIX}"
  printf "Dataset ID with region suffix: %s.\n" "${DATASET_ID}"
fi

DATASET_ID="${DATASET_ID}${SHORT_SHA}"

printf "*************** "
printf "Deploying Stored Procs in BigQuery dataset: %s." "${DATASET_ID}"
printf "***************\n"

if ! (dataform run . \
    --default-location=${BQ_LOCATION} \
    --default-database=${PROJECT_ID} \
    --default-schema=${DATASET_ID} \
    --vars=REGION_SUFFIX=${REGION_SUFFIX} \
    --timeout=10m); then
  # If any error occurs, delete BigQuery testing dataset before exiting with status code 1
  # If SHORT_SHA is not null, then we know a test dataset was used.
  if [[ -n "${SHORT_SHA}" ]]; then
    printf "Deleting BigQuery dataset %s error occurred while running 'dataform run'.\n" "${DATASET_ID}"
    bq --project_id "${PROJECT_ID}" rm -r -f --location "${BQ_LOCATION}" --dataset "${DATASET_ID}"
  fi
  printf "FAILURE: Encountered an error while running 'dataform run' on stored procedures.\n"
  exit 1
fi

if [[ -n "${SHORT_SHA}" ]]; then
  printf "Deleting BigQuery dataset %s because setting env var SHORT_SHA=%s means this is a test build.\n" "${DATASET_ID}" "${SHORT_SHA}"
  bq --project_id "${PROJECT_ID}" rm -r -f --location "${BQ_LOCATION}" --dataset "${DATASET_ID}"
fi
