#!/bin/bash

env=$1
tool=$2

terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}" > state.json
# terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir=/qa/terraform-sync-tool > state.json