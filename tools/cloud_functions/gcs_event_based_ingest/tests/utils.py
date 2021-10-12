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
"""Common test utilities"""
from time import monotonic  # Monotonic clock, cannot go backward.
from typing import List

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound

import gcs_ocn_bq_ingest.main

LOAD_JOB_POLLING_TIMEOUT = 120  # seconds


def trigger_gcf_for_each_blob(blobs: List[storage.Blob]):
    for blob in blobs:
        test_event = {
            "attributes": {
                "bucketId": blob.bucket.name,
                "objectId": blob.name
            }
        }
        gcs_ocn_bq_ingest.main.main(test_event, None)


def check_blobs_exist(blobs: List[storage.Blob], error_msg_if_missing=None):
    for blob in blobs:
        if not blob.exists():
            if error_msg_if_missing is None:
                error_msg_if_missing = (
                    f"{blob.name=} does not exist but was expected to exist.")
            raise NotFound(error_msg_if_missing)


def bq_wait_for_rows(bq_client: bigquery.Client, table: bigquery.Table,
                     expected_num_rows: int):
    """
  polls tables.get API for number of rows until reaches expected value or
  times out.

  This is mostly an optimization to speed up the test suite without making it
  flaky.
  """

    start_poll = monotonic()  # Monotonic clock, cannot go backward.
    actual_num_rows = 0
    while monotonic() - start_poll < LOAD_JOB_POLLING_TIMEOUT:
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
