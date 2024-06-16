#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cd $(dirname $0)
ROOT_DIR=$(pwd)

# unsetting exported variables
function unset_vars {
  unset JS_FILENAME
  unset MJS_FILENAME
  unset WASM_FILENAME
  unset SQL_FILENAME
  unset GCS_PATH
  unset ROOT_GCS_PATH
  unset EMSCRIPTEN_COMPILE_LOG
  unset BQ_LOG
}
#Example: ./install.sh -p nikunjbhartia-test-clients -d udaf_us_east7 -g gs://kll_sketch_udaf/webassembly_v
function help {
  echo "Usage: ./install.sh \\
        -p|--project-id <bq-project-id> \\
        -d|--dataset <bq-dataset> \\
        -g|--gcs-path <gs://bucket/[folder]> \\
        -s|--sketches <theta,kll,tuple>"
  unset_vars
  exit 1
}

SKETCHES_ARRAY=("theta" "kll" "tuple")
LOGS_DIR="$ROOT_DIR/logs"
rm -rf $LOGS_DIR

while (( "$#" )); do
  case "$1" in
    -p|--project-id)
      export BQ_PROJECT=$2
      shift 2
      ;;
    -d|--dataset)
      export BQ_DATASET=$2
      shift 2
      ;;
    -g|--gcs-path)
      #removing / at the end of path if present
      export ROOT_GCS_PATH=${2%/}
      shift 2
      ;;
    -s|--sketches)
      SKETCHES_RAW=$2
      IFS=',' read -ra SKETCHES_ARRAY <<< "$SKETCHES_RAW"

      # Validation logic (case-insensitive)
      for sketch in "${SKETCHES_ARRAY[@]}"; do
        shopt -s nocasematch  # Enable case-insensitive matching
        case "$sketch" in
          kll | theta | tuple)
              ;;  # Valid sketch
          *)
              echo "Error: Invalid sketch '$sketch'. Valid choices are {theta,kll,tuple}" >&2
              help
              ;;
        esac
        shopt -u nocasematch  # Turn off case-insensitive matching
      done

      shift 2
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      help
      ;;
  esac
done

if [[ -z "$BQ_PROJECT" ]] || [[ -z "$BQ_DATASET" ]] || [[ -z "$ROOT_GCS_PATH" ]]; then
    echo 'Error: Incorrect parameters'
    help
fi

#Takes in sketch type as a parameter : [kll, theta, tuple]
function modify_vars() {
  shopt -s nocasematch # Enable case-insensitive matching
  case $1 in
        theta)
            sketch_type="theta_sketch"
            export SKETCH_DIR="$ROOT_DIR/theta-sketch"
            ;;
        kll)
            sketch_type="kll_sketch"
            export SKETCH_DIR="$ROOT_DIR/kll-sketch"
            ;;
        tuple)
            sketch_type="tuple_sketch"
            export SKETCH_DIR="$ROOT_DIR/tuple-sketch"
            ;;
        *) # unsupported type
            echo "Error: Unsupported Sketch Type"
            help
            ;;
  esac
  shopt -u nocasematch  # Turn off case-insensitive matching
  mkdir -p "$LOGS_DIR/$sketch_type"
  mkdir -p "$SKETCH_DIR/compiled"

  export JS_FILENAME="$SKETCH_DIR/compiled/$sketch_type.js"
  export MJS_FILENAME="$SKETCH_DIR/compiled/$sketch_type.mjs"
  export WASM_FILENAME="$SKETCH_DIR/compiled/$sketch_type.wasm"
  export SQL_FILENAME="$SKETCH_DIR/$sketch_type.sql"
  export GCS_PATH="$ROOT_GCS_PATH/$sketch_type"
  export EMSCRIPTEN_COMPILE_LOG="$LOGS_DIR/$sketch_type/emscripten.log"
  export BQ_LOG="$LOGS_DIR/$sketch_type/bigquery.log"
}


#Takes in sketch type as a parameter : [kll, theta, tuple]
function install_sketch_type() {
  curr_sketch=$( echo $1 | tr '[:upper:]' '[:lower:]') #Translating upper case chars to lower case
  echo "*********************************************************"
  echo "Installing BQ UD(A)Fs for $curr_sketch Sketch                    "
  echo "*********************************************************"

  modify_vars $curr_sketch
  cd $SKETCH_DIR

  echo "Compiling CPP code to webassembly"

  make clean > $EMSCRIPTEN_COMPILE_LOG 2>&1
  make all > $EMSCRIPTEN_COMPILE_LOG 2>&1

  if [ ! -e "${JS_FILENAME}" -o  ! -e "${MJS_FILENAME}" -o  ! -e "${WASM_FILENAME}" ]; then
     echo "Step failed. Please report with logs : $EMSCRIPTEN_COMPILE_LOG"
     unset_vars
     exit 1
  fi

  echo "Webassembly compilation succeeded. Moving compiled files to GCS  "

  gcloud storage cp $JS_FILENAME "$GCS_PATH/" --project $BQ_PROJECT
  gcloud storage cp $MJS_FILENAME "$GCS_PATH/" --project $BQ_PROJECT
  gcloud storage cp $WASM_FILENAME "$GCS_PATH/" --project $BQ_PROJECT
  exit_code=$?

  if [ $exit_code -ne 0 ]; then
      echo "Step failed. Check if GCS Path $GCS_PATH exists and if have required access "
      echo "Consider executing 'gcloud auth login' if path is correct "
      unset_vars
      exit 1
  fi

  echo "GCS Upload Successful. Creating BigQuery UD(A)Fs  "

  bq --project_id=$BQ_PROJECT  query --nouse_legacy_sql "$(envsubst <$SQL_FILENAME)" > $BQ_LOG 2>&1
  exit_code=$?

  if [ $exit_code -ne 0 ]; then
    echo "Step failed. Please check logs : $BQ_LOG"
    unset_vars
    exit 1
  fi

  echo "****************************************************************************"
  echo "Installation Successful for $curr_sketch Sketch."
  echo "BQ UD(A)Fs created in dataset: $BQ_PROJECT.$BQ_DATASET"
  echo "Webassembly compiled files uploaded to $GCS_PATH/"
  echo "****************************************************************************"
  echo ""
}


for sketch in "${SKETCHES_ARRAY[@]}"; do
  install_sketch_type "$sketch"
done

unset_vars