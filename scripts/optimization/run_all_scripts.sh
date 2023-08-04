#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Run the table_read_patterns.sql file first because it's a dependency for
# some of the other scripts.
bq query --use_legacy_sql=false --nouse_cache < table_read_patterns.sql

# Run all the .sql files in the current directory
for f in *.sql; do
  if [ $f = "table_read_patterns.sql" ]; then
    # Skip this file, it's already been run
    continue
  fi
  bq query --use_legacy_sql=false --nouse_cache < $f
done
