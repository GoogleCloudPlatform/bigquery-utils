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

# Get input_table name as input 
for i in "$@"; do
  case $i in
    --input_table_name=*)
      input_table_name="${i#*=}"
      shift # past argument=value
      ;;
    --input_table_id_col_name=*)
      input_table_id_col_name="${i#*=}"
      shift # past argument=value
      ;;
    --input_table_query_text_col_name=*)
      input_table_query_text_col_name="${i#*=}"
      shift # past argument=value
      ;;
    --input_table_slots_col_name=*)
      input_table_slots_col_name="${i#*=}"
      shift # past argument=value
      ;;
    -*|--*)
      echo "Unknown option $i"
      exit 1
      ;;
    *)
      ;;
  esac
done

# Set the following flags for the bq command:
#   --quiet: suppress status updates while jobs are running
#   --nouse_legacy_sql: use standard SQL syntax
#   --nouse_cache: do not use cached results
bq_flags="--quiet --nouse_legacy_sql --nouse_cache"


# Run setup for anti pattern recognition tool
anti_pattern_recoginition_tool_tables_sql=$(sed -e "s/<input_table>/$input_table_name/g" \
                                                -e "s/<input_table_id_col_name>/$input_table_id_col_name/g" \
                                                -e "s/<input_table_query_text_col_name>/$input_table_query_text_col_name/g" \
                                                -e "s/<input_table_slots_col_name>/$input_table_slots_col_name/g" \
                                                "./anti-pattern-recognittion-tool-scripts/anti_pattern_recoginition_tool_tables.sql")

bq query ${bq_flags} <<< "$anti_pattern_recoginition_tool_tables_sql"

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
  update_queries_by_hash_w_anti_patterns_sql=$(sed -e "s/<input_table>/$input_table_name/g" \
                                                   -e "s/<input_table_id_col_name>/$input_table_id_col_name/g" \
                                                   "./anti-pattern-recognittion-tool-scripts/update_queries_by_hash_w_anti_patterns.sql")
  bq query ${bq_flags} <<< "$update_queries_by_hash_w_anti_patterns_sql"
  
} || { # catch
    echo 'Error: could not run Anti-pattern Recognition Tool. Try using GCP Cloud Shell https://cloud.google.com/shell/docs/launching-cloud-shell'
}

# Clean up anti pattern recognition tool
rm -rf bigquery-antipattern-recognition 