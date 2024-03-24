#!/bin/bash

# Copyright 2023 Google LLC
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

# Exit immediately if a command exits with a non-zero status.
set -e
# Set the following flags for the bq command:
#   --quiet: suppress status updates while jobs are running
#   --nouse_legacy_sql: use standard SQL syntax
#   --nouse_cache: do not use cached results
bq_flags="--quiet --nouse_legacy_sql --nouse_cache"


# Run setup for anti pattern recognition tool
bq query ${bq_flags} <anti_pattern_recoginition_tool_tables.sql

{ # try
  
  ## build anti-pattern recognition tool locally
  git clone https://github.com/GoogleCloudPlatform/bigquery-antipattern-recognition.git
  (cd bigquery-antipattern-recognition && mvn clean package jib:dockerBuild -DskipTests)
  
  ## build anti-pattern recognition tool locally
  export PROJECT_ID=$(gcloud config get-value project)
  docker run -i bigquery-antipattern-recognition \
  --input_bq_table ${PROJECT_ID}.optimization_workshop.antipattern_tool_input_view \
  --output_table ${PROJECT_ID}.optimization_workshop.antipattern_output_table

  # write anti pattern output to queries by has table
  bq query ${bq_flags} <update_queries_by_hash_w_anti_patterns.sql
  
} || { # catch
    echo 'Error: could not run Anti-pattern Recognition Tool. Try using GCP Cloud Shell https://cloud.google.com/shell/docs/launching-cloud-shell'
}

# Clean up anti pattern recognition tool
rm -rf bigquery-antipattern-recognition 