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
"""integration tests for gcs_ocn_bq_ingest"""
import os
import time

import google.cloud.exceptions
import pytest
from google.cloud import bigquery

import gcs_ocn_bq_ingest.main

TEST_DIR = os.path.realpath(os.path.dirname(__file__) + "/..")
LOAD_JOB_POLLING_TIMEOUT = 10  # seconds


@pytest.mark.IT
def test_load_job(bq, gcs_data, dest_dataset, dest_table, mock_env):
    """tests basic single invocation with load job"""
    if not gcs_data.exists():
        raise EnvironmentError("test data objects must exist")
    test_event = {
        "attributes": {
            "bucketId": gcs_data.bucket.name,
            "objectId": gcs_data.name
        }
    }
    gcs_ocn_bq_ingest.main.main(test_event, None)
    test_data_file = os.path.join(TEST_DIR, "resources", "test-data", "nation",
                                  "part-m-00001")
    expected_num_rows = sum(1 for _ in open(test_data_file))
    bq_wait_for_rows(bq, dest_table, expected_num_rows)


@pytest.mark.IT
def test_gcf_event_schema(bq, gcs_data, dest_dataset, dest_table, mock_env):
    """tests compatibility to Cloud Functions Background Function posting the
    storage object schema
    https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    directly based on object finalize.

    https://cloud.google.com/functions/docs/tutorials/storage#functions_tutorial_helloworld_storage-python
    """
    if not gcs_data.exists():
        raise EnvironmentError("test data objects must exist")
    test_event = {
        "kind": "storage#object",
        "bucket": gcs_data.bucket.name,
        "name": gcs_data.name,
    }
    gcs_ocn_bq_ingest.main.main(test_event, None)
    test_data_file = os.path.join(TEST_DIR, "resources", "test-data", "nation",
                                  "part-m-00001")
    expected_num_rows = sum(1 for _ in open(test_data_file))
    bq_wait_for_rows(bq, dest_table, expected_num_rows)


@pytest.mark.IT
def test_duplicate_notification(bq, gcs_data, dest_dataset, dest_table,
                                mock_env):
    """tests behavior with two notifications for the same success file."""
    if not gcs_data.exists():
        raise EnvironmentError("test data objects must exist")
    test_event = {
        "attributes": {
            "bucketId": gcs_data.bucket.name,
            "objectId": gcs_data.name
        }
    }
    gcs_ocn_bq_ingest.main.main(test_event, None)
    did_second_invocation_raise = False
    try:
        gcs_ocn_bq_ingest.main.main(test_event, None)
    except RuntimeError:
        did_second_invocation_raise = True
    assert did_second_invocation_raise

    test_data_file = os.path.join(TEST_DIR, "resources", "test-data", "nation",
                                  "part-m-00001")
    expected_num_rows = sum(1 for _ in open(test_data_file))
    bq_wait_for_rows(bq, dest_table, expected_num_rows)


@pytest.mark.IT
def test_load_job_truncating_batches(bq, gcs_batched_data,
                                     gcs_truncating_load_config, dest_dataset,
                                     dest_table, mock_env):
    """
    tests two successive batches with a load.json that dictates WRITE_TRUNCATE.

    after both load jobs the count should be the same as the number of lines
    in the test file because we should pick up the WRITE_TRUNCATE disposition.
    """
    if not gcs_truncating_load_config.exists():
        raise EnvironmentError(
            "the test is not configured correctly the load.json is missing")
    for gcs_data in gcs_batched_data:
        if not gcs_data.exists():
            raise EnvironmentError("test data objects must exist")
        test_event = {
            "attributes": {
                "bucketId": gcs_data.bucket.name,
                "objectId": gcs_data.name
            }
        }
        gcs_ocn_bq_ingest.main.main(test_event, None)
        test_data_file = os.path.join(TEST_DIR, "resources", "test-data",
                                      "nation", "part-m-00001")
        expected_num_rows = sum(1 for _ in open(test_data_file))
        bq_wait_for_rows(bq, dest_table, expected_num_rows)


@pytest.mark.IT
def test_load_job_appending_batches(bq, gcs_batched_data, dest_dataset,
                                    dest_table, mock_env):
    """
    tests two successive batches with the default load configuration.

    for each load the number of rows should increase by the number of rows
    in the test file because we should pick up the default WRITE_APPEND
    disposition.
    """
    test_data_file = os.path.join(TEST_DIR, "resources", "test-data", "nation",
                                  "part-m-00001")
    test_count = sum(1 for _ in open(test_data_file))
    expected_counts = [test_count, 2 * test_count]
    for i, gcs_data in enumerate(gcs_batched_data):
        if not gcs_data.exists():
            raise EnvironmentError("test data objects must exist")
        test_event = {
            "attributes": {
                "bucketId": gcs_data.bucket.name,
                "objectId": gcs_data.name
            }
        }
        gcs_ocn_bq_ingest.main.main(test_event, None)
        bq_wait_for_rows(bq, dest_table, expected_counts[i])


@pytest.mark.IT
def test_external_query(bq, gcs_data, gcs_external_config, dest_dataset,
                        dest_table, mock_env):
    """tests the basic external query ingrestion mechanics
    with bq_transform.sql and external.json
    """
    if not gcs_data.exists():
        raise google.cloud.exceptions.NotFound("test data objects must exist")
    if not all((blob.exists() for blob in gcs_external_config)):
        raise google.cloud.exceptions.NotFound("config objects must exist")

    test_event = {
        "attributes": {
            "bucketId": gcs_data.bucket.name,
            "objectId": gcs_data.name
        }
    }
    gcs_ocn_bq_ingest.main.main(test_event, None)
    test_data_file = os.path.join(TEST_DIR, "resources", "test-data", "nation",
                                  "part-m-00001")
    expected_num_rows = sum(1 for _ in open(test_data_file))
    bq_wait_for_rows(bq, dest_table, expected_num_rows)


@pytest.mark.IT
def test_load_job_partitioned(bq, gcs_partitioned_data,
                              gcs_truncating_load_config, dest_dataset,
                              dest_partitioned_table, mock_env):
    """
    Test loading separate partitions with WRITE_TRUNCATE

    after both load jobs the count should equal the sum of the test data in both
    partitions despite having WRITE_TRUNCATE disposition because the destination
    table should target only a particular partition with a decorator.
    """
    if not gcs_truncating_load_config.exists():
        raise EnvironmentError(
            "the test is not configured correctly the load.json is missing")
    test_event = {}
    # load each partition.
    for gcs_data in gcs_partitioned_data:
        if not gcs_data.exists():
            raise EnvironmentError("test data objects must exist")
        test_event = {
            "attributes": {
                "bucketId": gcs_data.bucket.name,
                "objectId": gcs_data.name
            }
        }
        gcs_ocn_bq_ingest.main.main(test_event, None)
    expected_num_rows = 0
    for part in [
            "$2017041101",
            "$2017041102",
    ]:
        test_data_file = os.path.join(TEST_DIR, "resources", "test-data",
                                      "nyc_311", part, "nyc_311.csv")
        expected_num_rows += sum(1 for _ in open(test_data_file))
    bq_wait_for_rows(bq, dest_partitioned_table, expected_num_rows)


def bq_wait_for_rows(bq_client: bigquery.Client, table: bigquery.Table,
                     expected_num_rows: int):
    """
    polls tables.get API for number of rows until reaches expected value or
    times out.

    This is mostly an optimization to speed up the test suite without making it
    flaky.
    """

    start_poll = time.monotonic()
    actual_num_rows = 0
    while time.monotonic() - start_poll < LOAD_JOB_POLLING_TIMEOUT:
        bq_table: bigquery.Table = bq_client.get_table(table)
        actual_num_rows = bq_table.num_rows
        if actual_num_rows == expected_num_rows:
            return
        if actual_num_rows > expected_num_rows:
            raise AssertionError(
                f"{table.project}.{table.dataset_id}.{table.table_id} has"
                f"{actual_num_rows} rows. expected {expected_num_rows} rows.")
    raise AssertionError(
        f"Timed out after {LOAD_JOB_POLLING_TIMEOUT} seconds waiting for "
        f"{table.project}.{table.dataset_id}.{table.table_id} to "
        f"reach {expected_num_rows} rows."
        f"last poll returned {actual_num_rows} rows.")
