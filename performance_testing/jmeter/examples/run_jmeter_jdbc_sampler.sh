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

apache-jmeter-5.4/bin/jmeter -n -l /tmp/sample_jmeter_log \
-t BigQuery-BI-and-ELT.jmx \
-Jproject_id=${PROJECT_ID} \
-Jpdt_csv_path=pdt_queries.csv \
-Jbq_public_csv_path=bigquery_public_data_bi_queries.csv \
-Jerror_csv_path=errors.csv \
-Jpdt_num_users=1 \
-Jbq_public_num_users=5 \
-Jnum_loops=1 \
-Jnum_slots=${SLOT_COUNT:=2000} \
-Jrun_id="test_run_$(date +%s)" \
-Jthread_duration=60 \
-Jramp_time=0;
