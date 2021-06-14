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
"""integration tests for the ordering behavior of backlog gcs_ocn_bq_ingest"""
import multiprocessing
import os
import queue
import time
from typing import Optional

import pytest
from google.cloud import bigquery
from google.cloud import storage

import gcs_ocn_bq_ingest.common.constants
import gcs_ocn_bq_ingest.common.ordering
import gcs_ocn_bq_ingest.common.utils
import gcs_ocn_bq_ingest.main

TEST_DIR = os.path.realpath(os.path.dirname(__file__) + "/..")
LOAD_JOB_POLLING_TIMEOUT = 20  # seconds

# Testing that the subscriber does not get choked up by a common race condition
# is crucial to ensuring this solution works.
# This parameter is for running the subscriber tests many times.
# During development it can be helpful to tweak this up or down as you are
# experimenting.
NUM_TRIES_SUBSCRIBER_TESTS = 25


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
        if gcs_data.name.endswith(
                gcs_ocn_bq_ingest.common.constants.SUCCESS_FILENAME):
            table_prefix = gcs_ocn_bq_ingest.common.utils.get_table_prefix(
                gcs_data.name)
            gcs_ocn_bq_ingest.common.ordering.backlog_publisher(gcs, gcs_data)

    expected_backlog_blobs = queue.Queue()
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041101",
        gcs_ocn_bq_ingest.common.constants.SUCCESS_FILENAME
    ]))
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041102",
        gcs_ocn_bq_ingest.common.constants.SUCCESS_FILENAME
    ]))

    for backlog_blob in gcs_bucket.list_blobs(
            prefix=f"{table_prefix}/_backlog"):
        assert backlog_blob.name == expected_backlog_blobs.get(block=False)

    backfill_blob: storage.Blob = gcs_bucket.blob(
        f"{table_prefix}/{gcs_ocn_bq_ingest.common.constants.BACKFILL_FILENAME}"
    )
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
        f"{table_prefix}/{gcs_ocn_bq_ingest.common.constants.BACKFILL_FILENAME}"
    )
    backfill_blob.upload_from_string("")
    backfill_blob.reload()
    original_backfill_blob_generation = backfill_blob.generation
    table_prefix = ""
    # load each partition.
    for gcs_data in gcs_partitioned_data:
        if not gcs_data.exists():
            raise EnvironmentError("test data objects must exist")
        if gcs_data.name.endswith(
                gcs_ocn_bq_ingest.common.constants.SUCCESS_FILENAME):
            table_prefix = gcs_ocn_bq_ingest.common.utils.get_table_prefix(
                gcs_data.name)
            gcs_ocn_bq_ingest.common.ordering.backlog_publisher(gcs, gcs_data)

    # Use of queue to test that list responses are returned in expected order.
    expected_backlog_blobs = queue.Queue()
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041101",
        gcs_ocn_bq_ingest.common.constants.SUCCESS_FILENAME
    ]))
    expected_backlog_blobs.put("/".join([
        table_prefix, "_backlog", "$2017041102",
        gcs_ocn_bq_ingest.common.constants.SUCCESS_FILENAME
    ]))

    for backlog_blob in gcs_bucket.list_blobs(
            prefix=f"{table_prefix}/_backlog"):
        assert backlog_blob.name == expected_backlog_blobs.get(block=False)

    backfill_blob.reload()
    assert backfill_blob.generation == original_backfill_blob_generation


@pytest.mark.IT
@pytest.mark.ORDERING
@pytest.mark.repeat(NUM_TRIES_SUBSCRIBER_TESTS)
def test_backlog_subscriber_in_order_with_new_batch_after_exit(
        bq, gcs, gcs_bucket, dest_dataset, dest_ordered_update_table,
        gcs_ordered_update_data, gcs_external_update_config, gcs_backlog,
        mock_env):
    """Test basic functionality of backlog subscriber.
    Populate a backlog with 3 files that make updates where we can assert
    that these jobs were applied in order.

    To ensure that the subscriber cleans up properly after itself before exit,
    we will drop a 4th batch after the subscriber has exited and assert that it
    gets applied as expected.
    """
    _run_subscriber(gcs, bq, gcs_external_update_config)
    table_prefix = gcs_ocn_bq_ingest.common.utils.get_table_prefix(
        gcs_external_update_config.name)
    backlog_blobs = gcs_bucket.list_blobs(prefix=f"{table_prefix}/_backlog/")
    assert backlog_blobs.num_results == 0, "backlog is not empty"
    bqlock_blob: storage.Blob = gcs_bucket.blob("_bqlock")
    assert not bqlock_blob.exists(), "_bqlock was not cleaned up"
    rows = bq.query("SELECT alpha_update FROM "
                    f"{dest_ordered_update_table.dataset_id}"
                    f".{dest_ordered_update_table.table_id}")
    expected_num_rows = 1
    num_rows = 0
    for row in rows:
        num_rows += 1
        assert row["alpha_update"] == "ABC", "backlog not applied in order"
    assert num_rows == expected_num_rows

    # Now we will test what happens when the publisher posts another batch after
    # the backlog subscriber has exited.
    backfill_blob = _post_a_new_batch(gcs_bucket, dest_dataset,
                                      dest_ordered_update_table)
    _run_subscriber(gcs, bq, backfill_blob)

    rows = bq.query("SELECT alpha_update FROM "
                    f"{dest_ordered_update_table.dataset_id}"
                    f".{dest_ordered_update_table.table_id}")
    expected_num_rows = 1
    num_rows = 0
    for row in rows:
        num_rows += 1
        assert row["alpha_update"] == "ABCD", "new incremental not applied"
    assert num_rows == expected_num_rows


@pytest.mark.IT
@pytest.mark.ORDERING
@pytest.mark.repeat(NUM_TRIES_SUBSCRIBER_TESTS)
def test_backlog_subscriber_in_order_with_new_batch_while_running(
        bq, gcs, gcs_bucket, dest_dataset, dest_ordered_update_table,
        gcs_ordered_update_data, gcs_external_update_config: storage.Blob,
        gcs_backlog, mock_env):
    """Test functionality of backlog subscriber when new batches are added
    before the subscriber is done finishing the existing backlog.

    Populate a backlog with 3 files that make updates where we can assert
    that these jobs were applied in order.
    In another process populate a fourth batch, and call the publisher.
    """
    # Cannot pickle clients to another process so we need to recreate some
    # objects without the client property.
    backfill_blob = storage.Blob.from_string(
        f"gs://{gcs_external_update_config.bucket.name}/"
        f"{gcs_external_update_config.name}")
    dataset = bigquery.Dataset.from_string(
        f"{dest_dataset.project}.{dest_dataset.dataset_id}")
    table = bigquery.Table.from_string(
        f"{dest_dataset.project}.{dest_dataset.dataset_id}."
        f"{dest_ordered_update_table.table_id}")
    bkt = storage.Bucket.from_string(f"gs://{gcs_bucket.name}")

    basename = os.path.basename(gcs_external_update_config.name)
    claim_blob: storage.Blob = gcs_external_update_config.bucket.blob(
        gcs_external_update_config.name.replace(
            basename, f"_claimed_{basename}_created_at_"
            f"{gcs_external_update_config.time_created.timestamp()}"))
    # Run subscriber w/ backlog and publisher w/ new batch in parallel.
    with multiprocessing.Pool(processes=3) as pool:
        res_subscriber = pool.apply_async(_run_subscriber,
                                          (None, None, backfill_blob))
        # wait for existence of claim blob to ensure subscriber is running.
        while not claim_blob.exists():
            pass
        res_backlog_publisher = pool.apply_async(_post_a_new_batch,
                                                 (bkt, dataset, table))
        res_backlog_publisher.wait()
        res_monitor = pool.apply_async(
            gcs_ocn_bq_ingest.common.ordering.subscriber_monitor,
            (None, bkt,
             f"{dataset.project}.{dataset.dataset_id}/{table.table_id}/"
             f"_backlog/04/_SUCCESS"))

        if res_monitor.get():
            print("subscriber monitor had to retrigger subscriber loop")
            backfill_blob.reload(client=gcs)
            _run_subscriber(None, None, backfill_blob)

        res_subscriber.wait()

    table_prefix = gcs_ocn_bq_ingest.common.utils.get_table_prefix(
        gcs_external_update_config.name)
    backlog_blobs = gcs_bucket.list_blobs(prefix=f"{table_prefix}/"
                                          f"_backlog/")
    assert backlog_blobs.num_results == 0, "backlog is not empty"
    bqlock_blob: storage.Blob = gcs_bucket.blob("_bqlock")
    assert not bqlock_blob.exists(), "_bqlock was not cleaned up"
    rows = bq.query("SELECT alpha_update FROM "
                    f"{dest_ordered_update_table.dataset_id}"
                    f".{dest_ordered_update_table.table_id}")
    expected_num_rows = 1
    num_rows = 0
    for row in rows:
        num_rows += 1
        assert row["alpha_update"] == "ABCD", "backlog not applied in order"
    assert num_rows == expected_num_rows


def _run_subscriber(
    gcs_client: Optional[storage.Client],
    bq_client: Optional[bigquery.Client],
    backfill_blob,
):
    gcs_ocn_bq_ingest.common.ordering.backlog_subscriber(
        gcs_client, bq_client, backfill_blob, time.monotonic())


def _post_a_new_batch(gcs_bucket, dest_dataset, dest_ordered_update_table):
    # We may run this in another process and cannot pickle client objects
    gcs = storage.Client()
    data_obj: storage.Blob
    for test_file in ["data.csv", "_SUCCESS"]:
        data_obj = gcs_bucket.blob("/".join([
            f"{dest_dataset.project}.{dest_dataset.dataset_id}",
            dest_ordered_update_table.table_id, "04", test_file
        ]))
        data_obj.upload_from_filename(os.path.join(TEST_DIR, "resources",
                                                   "test-data", "ordering",
                                                   "04", test_file),
                                      client=gcs)
    return gcs_ocn_bq_ingest.common.ordering.backlog_publisher(gcs, data_obj)
