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
import unittest
from typing import Dict, Optional
from unittest.mock import Mock

import pytest
from google.cloud import bigquery
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


def test_create_job_id():
    job_id = gcs_ocn_bq_ingest.common.utils.create_job_id(
        "bucket/source/dataset/table/2021/06/22/01/_SUCCESS")
    assert job_id.split('_SUCCESS')[
        0] == 'gcf-ingest-bucket-source-dataset-table-2021-06-22-01-'


def test_create_job_id_with_datasource_name_and_partition():
    table = bigquery.Table.from_string("project.dataset.table$2021062201")
    job_id = gcs_ocn_bq_ingest.common.utils.create_job_id(
        "bucket/source/dataset/table/2021/06/22/01/_SUCCESS", "source", table)
    job_id = '-'.join(job_id.split('-')[0:9])
    assert job_id == 'gcf-ingest-source-dataset-table-2021-06-22-01'


def test_create_job_id_with_datasource_name_and_partition_missing_hour():
    table = bigquery.Table.from_string("project.dataset.table$20210622")
    job_id = gcs_ocn_bq_ingest.common.utils.create_job_id(
        "bucket/source/dataset/table/2021/06/22/_SUCCESS", "source", table)
    job_id = '-'.join(job_id.split('-')[0:8])
    assert job_id == 'gcf-ingest-source-dataset-table-2021-06-22'


def test_create_job_id_with_datasource_name_and_no_partition():
    table = bigquery.Table.from_string("project.dataset.table")
    job_id = gcs_ocn_bq_ingest.common.utils.create_job_id(
        "bucket/source/dataset/table/_SUCCESS", "source", table)
    job_id = '-'.join(job_id.split('-')[0:5])
    assert job_id == 'gcf-ingest-source-dataset-table'


def test_compact_source_uris_with_wildcards():
    long_uris = [
        "gs://bucket/batch/file1.csv", "gs://bucket/batch/file2.csv",
        "gs://bucket/batch/file3.csv"
    ]
    source_uris = gcs_ocn_bq_ingest.common.utils.compact_source_uris_with_wildcards(
        long_uris)
    assert source_uris == ["gs://bucket/batch/*.csv"]


def test_compact_source_uris_with_wildcards_no_file_extension():
    long_uris_no_extension = [
        "gs://bucket/batch/file1", "gs://bucket/batch/file2",
        "gs://bucket/batch/file3", "gs://bucket/batch/file4.csv"
    ]
    source_uris = gcs_ocn_bq_ingest.common.utils.compact_source_uris_with_wildcards(
        long_uris_no_extension)
    unittest.TestCase().assertCountEqual(source_uris, [
        "gs://bucket/batch/file1", "gs://bucket/batch/file2",
        "gs://bucket/batch/file3", "gs://bucket/batch/*.csv"
    ])
