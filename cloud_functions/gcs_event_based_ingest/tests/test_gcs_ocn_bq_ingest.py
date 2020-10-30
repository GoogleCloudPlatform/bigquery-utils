# dataset/table/_SUCCESS
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""unit tests for gcs_ocn_bq_ingest"""
import os
import sys
import re

import pytest

sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
from gcs_ocn_bq_ingest import main

COMPILED_DEFAULT_DENTINATION_REGEX = re.compile(main.DEFAULT_DESTINATION_REGEX)


@pytest.mark.parametrize("test_input,expected", [
    # flat
    ("dataset/table/_SUCCESS",
     {
         "dataset": "dataset",
         "table": "table",
         "partition": None,
         "batch": None
     }),
    # partitioned
    ("dataset/table/$20201030/_SUCCESS",
     {
         "dataset": "dataset",
         "table": "table",
         "partition": "$20201030",
         "batch": None
     }),
    # partitioned batched
    ("dataset/table/$20201030/batch_id/_SUCCESS",
     {
         "dataset": "dataset",
         "table": "table",
         "partition": "$20201030",
         "batch": "batch_id"
     }),
    # batched no partition
    ("dataset/table/batch_id/_SUCCESS",
     {
         "dataset": "dataset",
         "table": "table",
         "partition": None,
         "batch": "batch_id"
     }),
])
def test_default_destination_regex(test_input, expected):
    """ensure our default regex handles each scenario we document"""
    assert COMPILED_DEFAULT_DENTINATION_REGEX.match(test_input).groupdict() == expected
