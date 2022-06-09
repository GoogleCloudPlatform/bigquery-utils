#!/bin/bash

env=$1
tool=$2

terragrunt run-all plan -json --terragrunt-non-interactive --terragrunt-working-dir="${env}"/"${tool}" > plan_out.json
