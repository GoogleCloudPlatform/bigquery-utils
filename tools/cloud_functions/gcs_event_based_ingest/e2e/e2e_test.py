# Copyright 2020 Google LLC.
# This software is provided as-is, without warranty or representation
# for any use or purpose.
# Your use of it is subject to your agreement with Google.

# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import concurrent.futures
import json
import time
from typing import Dict

import pytest
from google.cloud import bigquery
from google.cloud import storage

WAIT_FOR_ROWS_TIMEOUT = 180  # seconds


@pytest.mark.SYS
def test_gcs_ocn_bq_ingest_cloud_function(
    gcs: storage.Client,
    bq: bigquery.Client,
    tf_state: Dict,
    dest_table: bigquery.Table,
):
    """drop some test data and assert that the excpected actions are taken by
    the deployed cloud function"""
    input_bucket_id = tf_state['outputs']['bucket']['value']
    table_prefix = f"{dest_table.dataset_id}/" \
                   f"{dest_table.table_id}"
    extract_config = bigquery.ExtractJobConfig()
    extract_config.destination_format = bigquery.DestinationFormat.AVRO
    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))

    def _extract(batch: str):
        extract_job: bigquery.ExtractJob = bq.extract_table(
            public_table, f"gs://{input_bucket_id}/{table_prefix}/{batch}/"
            f"data-*.avro",
            job_config=extract_config)
        return extract_job.result()

    batches = [
        "historical/00", "historical/01", "historical/02", "incremental/03"
    ]
    history_batch_nums = ["00", "01", "02"]
    with concurrent.futures.ThreadPoolExecutor() as pool:
        # export some data from public BQ table into a historical partitions
        extract_results = pool.map(_extract, batches)

    for res in extract_results:
        assert res.errors is None, f"extract job {res.job_id} failed"

    bkt: storage.Bucket = gcs.lookup_bucket(input_bucket_id)
    # configure load jobs for this table
    load_config = bkt.blob(f"{table_prefix}/_config/load.json")
    load_config.upload_from_string(
        json.dumps({
            "writeDisposition": "WRITE_APPEND",
            "sourceFormat": "AVRO",
            "useAvroLogicalTypes": "True",
        }))
    # add historical success files
    for batch in history_batch_nums:
        historical_success_blob: storage.Blob = bkt.blob(
            f"{table_prefix}/historical/{batch}/_SUCCESS")
        historical_success_blob.upload_from_string("")

    # assert 0 bq rows (because _HISTORYDONE not dropped yet)
    dest_table: bigquery.Table = bq.get_table(dest_table)
    assert dest_table.num_rows == 0, \
        "history was ingested before _HISTORYDONE was uploaded"

    # add _HISTORYDONE
    history_done_blob: storage.Blob = bkt.blob(f"{table_prefix}/_HISTORYDONE")
    history_done_blob.upload_from_string("")

    # wait for bq rows to reach expected num rows
    bq_wait_for_rows(bq, dest_table,
                     public_table.num_rows * len(history_batch_nums))

    # add the incremental success file
    incremental_success_blob: storage.Blob = bkt.blob(
        f"{table_prefix}/{batches[-1]}/_SUCCESS")
    incremental_success_blob.upload_from_string("")

    # wait on new expected bq rows
    bq_wait_for_rows(bq, dest_table, public_table.num_rows * len(batches))


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
    while time.monotonic() - start_poll < WAIT_FOR_ROWS_TIMEOUT:
        bq_table: bigquery.Table = bq_client.get_table(table)
        actual_num_rows = bq_table.num_rows
        if actual_num_rows == expected_num_rows:
            return
        if actual_num_rows > expected_num_rows:
            raise AssertionError(
                f"{table.project}.{table.dataset_id}.{table.table_id} has"
                f"{actual_num_rows} rows. expected {expected_num_rows} rows.")
    raise AssertionError(
        f"Timed out after {WAIT_FOR_ROWS_TIMEOUT} seconds waiting for "
        f"{table.project}.{table.dataset_id}.{table.table_id} to "
        f"reach {expected_num_rows} rows."
        f"last poll returned {actual_num_rows} rows.")
