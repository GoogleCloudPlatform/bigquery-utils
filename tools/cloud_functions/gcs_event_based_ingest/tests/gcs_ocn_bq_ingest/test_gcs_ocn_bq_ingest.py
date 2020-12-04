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
import re
from typing import Dict, Optional

import pytest

import gcs_ocn_bq_ingest.constants
import gcs_ocn_bq_ingest.main
import gcs_ocn_bq_ingest.utils

COMPILED_DEFAULT_DENTINATION_REGEX = re.compile(
    gcs_ocn_bq_ingest.constants.DEFAULT_DESTINATION_REGEX)


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "dataset/table/_SUCCESS",  # flat
            {
                "dataset": "dataset",
                "table": "table",
                "partition": None,
                "yyyy": None,
                "mm": None,
                "dd": None,
                "hh": None,
                "batch": None
            }),
        (
            "dataset/table/$20201030/_SUCCESS",  # partitioned
            {
                "dataset": "dataset",
                "table": "table",
                "partition": "$20201030",
                "yyyy": None,
                "mm": None,
                "dd": None,
                "hh": None,
                "batch": None
            }),
        (
            "dataset/table/$20201030/batch_id/_SUCCESS",  # partitioned, batched
            {
                "dataset": "dataset",
                "table": "table",
                "partition": "$20201030",
                "yyyy": None,
                "mm": None,
                "dd": None,
                "hh": None,
                "batch": "batch_id"
            }),
        (
            "dataset/table/batch_id/_SUCCESS",  # batched (no partitioning)
            {
                "dataset": "dataset",
                "table": "table",
                "partition": None,
                "yyyy": None,
                "mm": None,
                "dd": None,
                "hh": None,
                "batch": "batch_id"
            }),
        ("dataset/table/2020/01/02/03/batch_id/_SUCCESS", {
            "dataset": "dataset",
            "table": "table",
            "partition": None,
            "yyyy": "2020",
            "mm": "01",
            "dd": "02",
            "hh": "03",
            "batch": "batch_id"
        }),
        ("project.dataset/table/2020/01/02/03/batch_id/_SUCCESS", {
            "dataset": "project.dataset",
            "table": "table",
            "partition": None,
            "yyyy": "2020",
            "mm": "01",
            "dd": "02",
            "hh": "03",
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
    assert gcs_ocn_bq_ingest.utils.flatten2dlist(test_input) == expected


@pytest.mark.parametrize(
    "original, update, expected",
    [
        # yapf: disable
        (  # empty original
            {}, {
                "a": 1
            }, {
                "a": 1
            }),
        (  # empty update
            {
                "a": 1
            }, {}, {
                "a": 1
            }),
        (  # basic update of top-level key
            {
                "a": 1
            }, {
                "a": 2
            }, {
                "a": 2
            }),
        (  # update of list
            {
                "a": [1]
            }, {
                "a": [2]
            }, {
                "a": [2]
            }),
        (  # update of nested key
            {
                "a": {
                    "b": 1
                }
            }, {
                "a": {
                    "b": 2
                }
            }, {
                "a": {
                    "b": 2
                }
            }),
        (  # don't drop keys that only appear in original
            {
                "a": {
                    "b": 1,
                    "c": 2
                },
                "d": 3
            }, {
                "a": {
                    "b": 4
                },
            }, {
                "a": {
                    "b": 4,
                    "c": 2
                },
                "d": 3
            }),
        # yapf: enable
    ])
def test_recursive_update(original, update, expected):
    assert gcs_ocn_bq_ingest.utils.recursive_update(original,
                                                    update) == expected
