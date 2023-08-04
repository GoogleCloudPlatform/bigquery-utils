#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Run all the .sql files in the current directory
for f in *.sql; do
  bq query --use_legacy_sql=false --nouse_cache < $f
done
