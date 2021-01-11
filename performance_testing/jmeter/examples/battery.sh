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

#!/bin/bash

set -ex

reservation_sizes="1000 750 500 250"
export USERS=10
export NUM_TEST_RUNS=${NUM_TEST_RUNS:=1}

if [ -z $PROJECT_ID ]; then
  echo "Please set PROJECT_ID to the project ID where you intend the queries to execute."
  exit 1
fi

if [ -z $BQ_ADMIN_PROJECT ]; then
  echo "Please set BQ_ADMIN_PROJECT to the project ID where your reservations are managed."
  exit 1
fi

if [ -z $BQ_RESERVATION_ID ]; then
  echo "Please set BQ_RESERVATION_ID to the reservation ID you want to vary between test runs."
  exit 1
fi

for r in $reservation_sizes; do
  echo "Setting slot reservation to $r slots for the next test."
  bq --project_id $BQ_ADMIN_PROJECT update --reservation --location=us --slots ${r} --nouse_idle_slots $BQ_RESERVATION_ID || exit 1
  for run in $(seq 1 $NUM_TEST_RUNS); do
    echo "starting test run $run of $NUM_TEST_RUNS."
    SLOT_COUNT=$r time ./run_jmeter_jdbc_sampler.sh
  done
done
