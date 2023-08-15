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

# The following script retrieves all distinct projects from the JOBS_BY_ORGANIZATION view
# and then enables the recommender API for each project.
# This is useful for when you have a large number of projects and you want to enable the
# recommender API for all of them.
projects=$(
  bq query \
    --nouse_legacy_sql \
    --format=csv \
    "SELECT DISTINCT project_id FROM \`region-us\`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION" |
    sed 1d
)
for proj in $projects; do
  gcloud services --project="${proj}" enable recommender.googleapis.com &
done
