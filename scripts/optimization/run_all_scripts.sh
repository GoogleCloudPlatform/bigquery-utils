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

# Run all the .sql files in the current directory in parallel,
# except for table_read_patterns.sql
# and freq_read_tables_without_partitioning_or_clustering.sql
# since they'll be run sequentially due to the depedency between them.
for f in *.sql; do
  if [[ $f = "table_read_patterns.sql" ||
    $f = "freq_read_tables_without_partitioning_or_clustering.sql" ]]; then
    # Skip this file, it's already been run
    continue
  fi
  bq query --use_legacy_sql=false --nouse_cache < $f &
done

# Run the table_read_patterns.sql file first because it's a dependency for
# freq_read_tables_without_partitioning_or_clustering.sql
bq query --use_legacy_sql=false --nouse_cache <table_read_patterns.sql
bq query --use_legacy_sql=false --nouse_cache <freq_read_tables_without_partitioning_or_clustering.sql &
