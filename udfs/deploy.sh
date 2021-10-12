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

if [[ -n "${PROJECT_ID}" ]]; then
  (cd tests/dataform_testing_framework && ./deploy_and_run_tests.sh)
elif [[ -n $(gcloud config get-value project) ]]; then
  export PROJECT_ID=$(gcloud config get-value project)
  printf "Env variable PROJECT_ID is not set. Retrieving default value from current gcloud configuration.\n"
  printf "Running with PROJECT_ID=%s.\n" "${PROJECT_ID}"
  (cd tests/dataform_testing_framework && ./deploy_and_run_tests.sh)
else
  printf "Set env variable PROJECT_ID to your own project id.\n"
  printf "For example, run the following to set PROJECT_ID:\n export PROJECT_ID=YOUR_PROJ_ID\n"
fi
