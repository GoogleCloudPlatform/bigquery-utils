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

# Directory of this script
SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )"

# Directory of the UDFs
UDF_DIR=udfs

# Location for new datasets
LOCATION=US

# Set colors if terminal supports it
ncolors=$(tput colors)
if [[ $ncolors -gt 7 ]]; then
  BOLD=$(tput bold)
  NORMAL=$(tput sgr0)
fi

#######################################
# Retrieves the dataset of a file by
# evaluating the mapping of the file's
# parent directory to an associated
# dataset.
# Globals:
#   None
# Arguments:
#   file
# Returns:
#   The dataset
#######################################
function get_dataset() {
  local file=$1
  local dataset=""
  case $file in
    *"community"*)
      dataset="fn";;
    *"netezza"*)
      dataset="nz";;
    *"oracle"*)
      dataset="or";;
    *"redshift"*)
      dataset="rs";;
    *"teradata"*)
      dataset="td";;
    *"vertica"*)
      dataset="ve";;
  esac
  echo "$dataset"
}


#######################################
# Creates a dataset if it doesn't exist
# already.
# Globals:
#   LOCATION
# Arguments:
#   dataset
# Returns:
#   None
#######################################
function create_dataset_if_not_exists() {
  local dataset=$1
  # If we failed to show the dataset, it doesn't exist so create it.
  if ! bq show --headless $dataset > /dev/null 2>&1; then
    bq mk --headless -d --data_location=$LOCATION $dataset
  fi
}


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
# Returns:
#   None
#######################################
function execute_query() {
  local file=$1
  local dry_run=$2
  local dataset=$3

  printf "${BOLD}${file}${NORMAL}\n"
  if [[ $dry_run == true ]]; then
    bq query --dataset_id $dataset --headless --nouse_legacy_sql --dry_run "$(cat $file)"
  else
    bq query --dataset_id $dataset --headless --nouse_legacy_sql "$(cat $file)"
  fi

  if [[ $? -gt 0 ]]; then
    printf "Failed to create: $file"
    exit 1
  fi
}


#######################################
# Executes dry-runs of all SQL scripts
# within the repository.
# Globals:
#   None
# Arguments:
#   None
# Returns:
#   None
#######################################
function build() {
  local sql_files=$(find . -type f -name "*.sql")
  local num_files=$(echo "$sql_files" | wc -l)

  printf "Building $num_files database objects...\n"
  while read -r file; do
    local dataset=$(get_dataset $file)

    if [[ ! -z $dataset ]]; then
      create_dataset_if_not_exists $dataset
      execute_query $file true $dataset
    fi
  done <<< "$sql_files"
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
function deploy() {
  local sql_files=$(find $UDF_DIR -type f -name "*.sql")
  local num_files=$(echo "$sql_files" | wc -l)

  printf "Creating or updating $num_files database objects...\n"
  while read -r file; do
    local dataset=$(get_dataset $file)

    if [[ -n $dataset ]]; then
      create_dataset_if_not_exists $dataset
      execute_query $file false $dataset
    fi
  done <<< "$sql_files"
}


#######################################
# Main entry-point for execution
# Globals:
#   SCRIPT_DIR
#   UDF_DIR
# Arguments:
#   BRANCH_NAME
#   _PR_NUMBER
# Returns:
#   None
#######################################
function main() {
  cd $SCRIPT_DIR/.. || exit
  local branch=$1
  local pull_request_num=$2
  printf "Branch: %s\n" "$branch"
  printf "Pull Request #: %s\n" "$pull_request_num"

  if [[ "$branch" == "master" && -z "$pull_request_num" ]]; then
    deploy
  else
    build
  fi
}

main "$@"
