# dataset/table/_SUCCESS
# Copyright 2021 Google LLC
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
import time
from typing import Dict, Optional
from unittest.mock import Mock

import pytest
from google.cloud import storage

import gcs_ocn_bq_ingest.common.constants
import gcs_ocn_bq_ingest.common.utils
import gcs_ocn_bq_ingest.main

COMPILED_DEFAULT_DENTINATION_REGEX = re.compile(
    gcs_ocn_bq_ingest.common.constants.DEFAULT_DESTINATION_REGEX)


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
        ("project.dataset/table/historical/2020/01/02/03/batch_id/_SUCCESS", {
            "dataset": "project.dataset",
            "table": "table",
            "partition": None,
            "yyyy": "2020",
            "mm": "01",
            "dd": "02",
            "hh": "03",
            "batch": "batch_id"
        }),
        ("project.dataset/table/incremental/2020/01/02/04/batch_id/_SUCCESS", {
            "dataset": "project.dataset",
            "table": "table",
            "partition": None,
            "yyyy": "2020",
            "mm": "01",
            "dd": "02",
            "hh": "04",
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
    assert gcs_ocn_bq_ingest.common.utils.flatten2dlist(test_input) == expected


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
    assert gcs_ocn_bq_ingest.common.utils.recursive_update(original,
                                                           update) == expected


@pytest.mark.parametrize(
    "test_input,expected",
    [
        (
            "dataset/table/_SUCCESS",  # flat
            "dataset/table"),
        (
            "dataset/table/$20201030/_SUCCESS",  # partitioned
            "dataset/table"),
        (
            "dataset/table/$20201030/batch_id/_SUCCESS",  # partitioned, batched
            "dataset/table"),
        (
            "dataset/table/batch_id/_SUCCESS",  # batched (no partitioning)
            "dataset/table"),
        ("dataset/table/2020/01/02/03/batch_id/_SUCCESS", "dataset/table"),
        ("project.dataset/table/2020/01/02/03/batch_id/_SUCCESS",
         "project.dataset/table"),
        ("dataset/table/_BACKFILL", "dataset/table"),
        ("dataset/table/_bqlock", "dataset/table"),
        ("dataset/table/_backlog/2020/01/02/03/_SUCCESS", "dataset/table"),
    ])
def test_get_table_prefix(test_input, expected):
    assert gcs_ocn_bq_ingest.common.utils.get_table_prefix(
        test_input) == expected


def test_triage_event(mock_env, mocker):
    test_event_blob: storage.Blob = storage.Blob.from_string(
        "gs://foo/bar/baz/00/_SUCCESS")
    apply_mock = mocker.patch('gcs_ocn_bq_ingest.common.utils.apply')
    bq_mock = Mock()
    bq_mock.project = "foo"
    gcs_ocn_bq_ingest.main.triage_event(None, bq_mock, test_event_blob,
                                        time.monotonic())
    apply_mock.assert_called_once()


def test_triage_event_ordered(ordered_mock_env, mocker):
    enforce_ordering = True
    test_event_blob: storage.Blob = storage.Blob.from_string(
        "gs://foo/bar/baz/00/_SUCCESS")
    apply_mock = mocker.patch('gcs_ocn_bq_ingest.common.utils.apply')
    publisher_mock = mocker.patch(
        'gcs_ocn_bq_ingest.common.ordering.backlog_publisher')
    bq_mock = Mock()
    bq_mock.project = "foo"
    gcs_ocn_bq_ingest.main.triage_event(None,
                                        bq_mock,
                                        test_event_blob,
                                        time.monotonic(),
                                        enforce_ordering=enforce_ordering)
    publisher_mock.assert_called_once()

    test_event_blob: storage.Blob = storage.Blob.from_string(
        "gs://foo/bar/baz/_BACKFILL")
    subscriber_mock = mocker.patch(
        'gcs_ocn_bq_ingest.common.ordering.backlog_subscriber')
    gcs_ocn_bq_ingest.main.triage_event(None,
                                        None,
                                        test_event_blob,
                                        time.monotonic(),
                                        enforce_ordering=enforce_ordering)
    subscriber_mock.assert_called_once()

    test_event_blob: storage.Blob = storage.Blob.from_string(
        "gs://foo/bar/baz/_backlog/00/_SUCCESS")
    monitor_mock = mocker.patch(
        'gcs_ocn_bq_ingest.common.ordering.subscriber_monitor')
    gcs_ocn_bq_ingest.main.triage_event(None,
                                        None,
                                        test_event_blob,
                                        time.monotonic(),
                                        enforce_ordering=enforce_ordering)
    monitor_mock.assert_called_once()
    apply_mock.assert_not_called()
