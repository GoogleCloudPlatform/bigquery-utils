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


if [[ -n "${JS_BUCKET}" ]]; then
  gcloud builds submit . --substitutions _BQ_LOCATION="${BQ_LOCATION}",SHORT_SHA=_test_env,_JS_BUCKET="${JS_BUCKET}"
else
  printf "Set env variable JS_BUCKET to your own GCS bucket where Javascript libraries can be deployed.\n"
  printf "For example, run the following to set JS_BUCKET:\n export JS_BUCKET=gs://YOUR_BUCKET/PATH/TO/LIBS\n"
fi
