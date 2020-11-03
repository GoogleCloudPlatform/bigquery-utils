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
import re
import sys
from typing import Dict, Optional

import pytest

sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
from gcs_ocn_bq_ingest import main

COMPILED_DEFAULT_DENTINATION_REGEX = re.compile(main.DEFAULT_DESTINATION_REGEX)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "dataset/table/_SUCCESS",    # flat
            {
                "dataset": "dataset",
                "table": "table",
                "partition": None,
                "batch": None
            }),
        (
            "dataset/table/$20201030/_SUCCESS",    # partitioned
            {
                "dataset": "dataset",
                "table": "table",
                "partition": "$20201030",
                "batch": None
            }),
        (
            "dataset/table/$20201030/batch_id/_SUCCESS",    # partitioned, batched
            {
                "dataset": "dataset",
                "table": "table",
                "partition": "$20201030",
                "batch": "batch_id"
            }),
        (
            "dataset/table/batch_id/_SUCCESS",    # batched (no partitioning)
            {
                "dataset": "dataset",
                "table": "table",
                "partition": None,
                "batch": "batch_id"
            }),
    ])
def test_default_destination_regex(test_input: str,
                                   expected: Dict[str, Optional[str]]):
    """ensure our default regex handles each scenarios we document.
    this test is to support improving this regex in the future w/o regressing
    for existing use cases.
    """
    match = COMPILED_DEFAULT_DENTINATION_REGEX.match(test_input)
    if match:
        assert match.groupdict() == expected
    else:
        raise AssertionError(f"{COMPILED_DEFAULT_DENTINATION_REGEX}"
                             f" did not match test case {test_input}.")


@pytest.mark.parametrize("test_input,expected", [
    ([], []),
    ([[]], []),
    ([["foo"], ["bar", "baz"]], ["foo", "bar", "baz"]),
    ([["foo"], []], ["foo"]),
    ([["foo"], [], ["bar", "baz"]], ["foo", "bar", "baz"]),
])
def test_flattend2dlist(test_input, expected):
    assert main.flatten2dlist(test_input) == expected
