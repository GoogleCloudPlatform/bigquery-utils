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

# Run all the .sql files in the current directory in parallel,
# except for table_read_patterns.sql
# and actively_read_tables_with_partitioning_clustering_info.sql
# since they'll be run sequentially due to the depedency between them.
for f in *.sql; do
  if [[ $f = "table_read_patterns.sql" ||
    $f = "actively_read_tables_with_partitioning_clustering_info.sql" ]]; then
    # Skip this file, it's already been run
    continue
  fi
  bq query ${bq_flags} < $f &
done

# Run the table_read_patterns.sql file first because it's a dependency for
# actively_read_tables_with_partitioning_clustering_info.sql
bq query ${bq_flags} <table_read_patterns.sql
bq query ${bq_flags} <actively_read_tables_with_partitioning_clustering_info.sql &

