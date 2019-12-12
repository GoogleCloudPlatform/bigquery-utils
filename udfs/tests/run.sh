#!/usr/bin/env bash

# Copyright 2019 Google LLC
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

#########################################################################
# Call run.sh locally with an optional parameter specifying the test name
# if you want to run only that test and no others
#
# e.g. bash udfs/tests/run.sh test_community_udf
#
#########################################################################
python3 -m pip install -r udfs/tests/requirements.txt
python3 -m pytest -v --workers 100 udfs/tests/create_udf_signatures.py
python3 -m pytest -v --workers 100 udfs/tests/test_create_udfs.py
python3 -m pytest -v --workers 100 udfs/tests/test_run_udfs.py