#!/usr/bin/env bash
set -eo pipefail

git clone --recursive --single-branch --branch $(cat VERSION.txt) https://github.com/apache/datasketches-bigquery.git
cd datasketches-bigquery

_BQ_DATASET="datasketches"
if [[ "${_BQ_LOCATION^^}" != "US" ]]; then
  printf "Deploying to regional BigQuery location: %s\n" "${_BQ_LOCATION}"
  _BQ_DATASET="${_BQ_DATASET}_$(echo $_BQ_LOCATION | tr '[:upper:]' '[:lower:]' | tr '-' '_')"
  printf "BigQuery regional dataset name: %s\n" "${_BQ_DATASET}"
fi

gcloud builds submit . \
  --project=$PROJECT_ID \
  --region="us-central1" \
  --worker-pool="projects/${PROJECT_ID}/locations/us-central1/workerPools/udf-unit-testing" \
  --polling-interval="10" \
  --substitutions=_BQ_LOCATION=$_BQ_LOCATION,_BQ_DATASET=$_BQ_DATASET,_JS_BUCKET=$_JS_BUCKET
