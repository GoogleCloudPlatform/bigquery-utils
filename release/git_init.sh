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

# Only compare with master branch if this build has been triggered
# by either a non-master branch on origin repo or a pull request.
if [[ ! "${BRANCH_NAME}" = "master" || -n "${_PR_NUMBER}" ]]; then
  git init
  git config user.email "builder@bigquery-utils.repo"
  git config user.name "builder"
  printf "Adding repo %s as a remote" "${_REPO_URL}"
  git remote remove origin # Removing necessary when testing locally
  git remote add origin "${_REPO_URL}"
  git fetch origin master

  # Fetch the pull request which triggered this build and then
  # hard reset to it. The build.sh script can then call "git diff"
  # to figure out what to build based on what's changed.
  if [[ ! "${BRANCH_NAME}" = "master" && -z "${_PR_NUMBER}" ]]; then
    printf "Fetching and --hard resetting to (%s) branch from origin repo." "${BRANCH_NAME}"
    git fetch origin "${BRANCH_NAME}"
    git reset --hard origin/"${BRANCH_NAME}"
  elif [[ -n "${_PR_NUMBER}" ]]; then
    printf "Fetching and --hard resetting to the merge commit of pull request #%s which triggered this build." "${_PR_NUMBER}"
    git fetch origin +refs/pull/"${_PR_NUMBER}"/head:refs/remotes/origin/pr/"${_PR_NUMBER}"
    git reset --hard origin/pr/"${_PR_NUMBER}"
  fi
fi
