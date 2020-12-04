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
"""integration tests for the ordering behavior of backlog gcs_ocn_bq_ingest"""
import os
import queue
import time

import pytest
from google.cloud import storage

import gcs_ocn_bq_ingest.constants
import gcs_ocn_bq_ingest.main
import gcs_ocn_bq_ingest.ordering
import gcs_ocn_bq_ingest.utils

TEST_DIR = os.path.realpath(os.path.dirname(__file__) + "/..")
LOAD_JOB_POLLING_TIMEOUT = 20  # seconds


@pytest.mark.IT
@pytest.mark.ORDERING
def test_backlog_publisher(gcs, gcs_bucket, gcs_partitioned_data, mock_env):
    """Test basic functionality of backlog_publisher
    Drop two success files.
    Assert that both success files are added to backlog and backfill file
    created.
    Assert that that only one backfill file is not recreated.
    """
    table_prefix = ""
    # load each partition.
    for gcs_data in gcs_partitioned_data:
        if not gcs_data.exists():
            raise EnvironmentError("test data objects must exist")
        if gcs_data.name.endswith(gcs_ocn_bq_ingest.constants.SUCCESS_FILENAME):
            table_prefix = gcs_ocn_bq_ingest.utils.get_table_prefix(
                gcs_data.name)
            gcs_ocn_bq_ingest.ordering.backlog_publisher(gcs, gcs_data)

    expected_backlog_blobs = queue.Queue()
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041101",
        gcs_ocn_bq_ingest.constants.SUCCESS_FILENAME
    ]))
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041102",
        gcs_ocn_bq_ingest.constants.SUCCESS_FILENAME
    ]))

    for backlog_blob in gcs_bucket.list_blobs(
            prefix=f"{table_prefix}/_backlog"):
        assert backlog_blob.name == expected_backlog_blobs.get(block=False)

    backfill_blob: storage.Blob = gcs_bucket.blob(
        f"{table_prefix}/{gcs_ocn_bq_ingest.constants.BACKFILL_FILENAME}")
    assert backfill_blob.exists()


@pytest.mark.IT
@pytest.mark.ORDERING
def test_backlog_publisher_with_existing_backfill_file(gcs, gcs_bucket,
                                                       dest_dataset,
                                                       dest_partitioned_table,
                                                       gcs_partitioned_data,
                                                       mock_env):
    """Test basic functionality of backlog_publisher when the backfill is
    already running. It should not repost this backfill file.
    """
    table_prefix = "/".join(
        [dest_dataset.dataset_id, dest_partitioned_table.table_id])
    backfill_blob: storage.Blob = gcs_bucket.blob(
        f"{table_prefix}/{gcs_ocn_bq_ingest.constants.BACKFILL_FILENAME}")
    backfill_blob.upload_from_string("")
    backfill_blob.reload()
    original_backfill_blob_generation = backfill_blob.generation
    table_prefix = ""
    # load each partition.
    for gcs_data in gcs_partitioned_data:
        if not gcs_data.exists():
            raise EnvironmentError("test data objects must exist")
        if gcs_data.name.endswith(gcs_ocn_bq_ingest.constants.SUCCESS_FILENAME):
            table_prefix = gcs_ocn_bq_ingest.utils.get_table_prefix(
                gcs_data.name)
            gcs_ocn_bq_ingest.ordering.backlog_publisher(gcs, gcs_data)

    # Use of queue to test that list responses are returned in expected order.
    expected_backlog_blobs = queue.Queue()
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041101",
        gcs_ocn_bq_ingest.constants.SUCCESS_FILENAME
    ]))
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041102",
        gcs_ocn_bq_ingest.constants.SUCCESS_FILENAME
    ]))

    for backlog_blob in gcs_bucket.list_blobs(
            prefix=f"{table_prefix}/_backlog"):
        assert backlog_blob.name == expected_backlog_blobs.get(block=False)

    backfill_blob.reload()
    assert backfill_blob.generation == original_backfill_blob_generation


@pytest.mark.IT
@pytest.mark.ORDERING
def test_single_backlog_subscriber_in_order(bq, gcs, gcs_bucket, error,
                                            dest_ordered_update_table,
                                            gcs_ordered_update_data,
                                            gcs_external_update_config,
                                            gcs_backlog, mock_env):
    """Test basic functionality of backlog subscriber.
    Populate a backlog with 3 files that make updates where we can assert
    that these jobs were applied in order.
    """
    gcs_ocn_bq_ingest.ordering.backlog_subscriber(gcs, bq, error,
                                                  gcs_external_update_config,
                                                  time.monotonic())
    backlog_blobs = gcs_bucket.list_blobs(
        prefix=f"{gcs_ocn_bq_ingest.utils.get_table_prefix(gcs_external_update_config.name)}/_backlog/"
    )
    assert backlog_blobs.num_results == 0, "backlog is not empty"
    rows = bq.query("SELECT alpha_update FROM "
                    f"{dest_ordered_update_table.dataset_id}"
                    f".{dest_ordered_update_table.table_id}")
    expected_num_rows = 1
    num_rows = 0
    for row in rows:
        num_rows += 1
        assert row["alpha_update"] == "ABC", "incrementals not applied in order"
    assert num_rows == expected_num_rows
