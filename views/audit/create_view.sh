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

#######################################################
# Main entry-point for execution
# Environment Variables:
#   PROJECT_ID
#   DATASET_ID
# Arguments:
#   bq_view_sql_file - File name of the SQL view
# Returns:
#   None
#######################################################
function main() {
  local bq_view_sql_file=$1
  if [[ -z $bq_view_sql_file ]]; then
      printf "Must the file name of the SQL view as an argument."
      exit 1
  fi
  # The following line replaces all occurrences of "project_id.dataset_id"
  # with the two environment variables:
  #   - PROJECT_ID
  #   - DATASET_ID
  local view_sql
  view_sql=$(sed "s/project_id.dataset_id/${PROJECT_ID}.${DATASET_ID}/" < "${bq_view_sql_file}")
  # Run the view SQL in BigQuery to create it
  bq query --nouse_legacy_sql "${view_sql}"

}

main "$@"
