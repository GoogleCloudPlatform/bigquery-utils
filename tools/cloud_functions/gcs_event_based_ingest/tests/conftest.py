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
"""Integration tests for gcs_ocn_bq_ingest"""
import json
import os
import time
import uuid
from typing import List

import pytest
from google.cloud import bigquery
from google.cloud import error_reporting
from google.cloud import storage

import gcs_ocn_bq_ingest.ordering
import gcs_ocn_bq_ingest.utils

TEST_DIR = os.path.realpath(os.path.dirname(__file__))
LOAD_JOB_POLLING_TIMEOUT = 10  # seconds


@pytest.fixture(scope="module")
def bq() -> bigquery.Client:
    """BigQuery Client"""
    return bigquery.Client(location="US")


@pytest.fixture(scope="module")
def gcs() -> storage.Client:
    """GCS Client"""
    return storage.Client()


@pytest.fixture(scope="module")
def error() -> error_reporting.Client:
    """GCS Client"""
    return error_reporting.Client()


@pytest.fixture
def gcs_bucket(request, gcs) -> storage.bucket.Bucket:
    """GCS bucket for test artifacts"""
    bucket = gcs.create_bucket(str(uuid.uuid4()))
    # overide default field delimiter at bucket level
    load_config_json = {
        "fieldDelimiter": "|",
    }
    load_json_blob: storage.Blob = bucket.blob("_config/load.json")
    load_json_blob.upload_from_string(json.dumps(load_config_json))

    def teardown():
        load_json_blob.delete()
        bucket.delete(force=True)

    request.addfinalizer(teardown)

    return bucket


@pytest.fixture
def mock_env(gcs, monkeypatch):
    """environment variable mocks"""
    # Infer project from ADC of gcs client.
    monkeypatch.setenv("GCP_PROJECT", gcs.project)
    monkeypatch.setenv("FUNCTION_NAME", "integration-test")
    monkeypatch.setenv("FUNCTION_TIMEOUT_SEC", "120")


@pytest.fixture
def dest_dataset(request, bq, mock_env, monkeypatch):
    random_dataset = f"test_bq_ingest_gcf_{str(uuid.uuid4())[:8].replace('-','_')}"
    dataset = bigquery.Dataset(f"{os.getenv('GCP_PROJECT')}"
                               f".{random_dataset}")
    dataset.location = "US"
    bq.create_dataset(dataset)
    monkeypatch.setenv("BQ_LOAD_STATE_TABLE",
                       f"{dataset.dataset_id}.serverless_bq_loads")
    print(f"created dataset {dataset.dataset_id}")

    def teardown():
        bq.delete_dataset(dataset, delete_contents=True, not_found_ok=True)

    request.addfinalizer(teardown)
    return dataset


@pytest.fixture
def dest_table(request, bq, mock_env, dest_dataset) -> bigquery.Table:
    with open(os.path.join(TEST_DIR, "resources",
                           "nation_schema.json")) as schema_file:
        schema = gcs_ocn_bq_ingest.utils.dict_to_bq_schema(
            json.load(schema_file))

    table = bigquery.Table(
        f"{os.environ.get('GCP_PROJECT')}.{dest_dataset.dataset_id}.cf_test_nation",
        schema=schema,
    )

    table = bq.create_table(table)

    def teardown():
        bq.delete_table(table, not_found_ok=True)

    request.addfinalizer(teardown)
    return table


@pytest.fixture(scope="function")
def gcs_data(request, gcs_bucket, dest_dataset,
             dest_table) -> storage.blob.Blob:
    data_objs = []
    for test_file in ["part-m-00000", "part-m-00001", "_SUCCESS"]:
        data_obj: storage.blob.Blob = gcs_bucket.blob("/".join([
            f"{dest_dataset.project}.{dest_dataset.dataset_id}",
            dest_table.table_id, test_file
        ]))
        data_obj.upload_from_filename(
            os.path.join(TEST_DIR, "resources", "test-data", "nation",
                         test_file))
        data_objs.append(data_obj)

    def teardown():
        for do in data_objs:
            if do.exists:
                do.delete()

    request.addfinalizer(teardown)
    return data_objs[-1]


@pytest.fixture(scope="function")
def gcs_data_under_sub_dirs(request, gcs_bucket, dest_dataset,
                            dest_table) -> storage.blob.Blob:
    data_objs = []
    for test_file in ["part-m-00000", "part-m-00001", "_SUCCESS"]:
        data_obj: storage.blob.Blob = gcs_bucket.blob("/".join([
            f"{dest_dataset.project}.{dest_dataset.dataset_id}",
            dest_table.table_id, "foo", "bar", "baz", test_file
        ]))
        data_obj.upload_from_filename(
            os.path.join(TEST_DIR, "resources", "test-data", "nation",
                         test_file))
        data_objs.append(data_obj)

    def teardown():
        for do in data_objs:
            if do.exists():
                do.delete()

    request.addfinalizer(teardown)
    return data_objs[-1]


@pytest.fixture(scope="function")
def gcs_truncating_load_config(request, gcs_bucket, dest_dataset,
                               dest_table) -> storage.blob.Blob:
    config_obj: storage.blob.Blob = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id,
        dest_table.table_id,
        "_config",
        "load.json",
    ]))
    config_obj.upload_from_string(
        json.dumps({"writeDisposition": "WRITE_TRUNCATE"}))

    def teardown():
        if config_obj.exists():
            config_obj.delete()

    request.addfinalizer(teardown)
    return config_obj


@pytest.fixture(scope="function")
def gcs_batched_data(request, gcs_bucket, dest_dataset,
                     dest_table) -> List[storage.blob.Blob]:
    """
  upload two batches of data
  """
    data_objs = []
    for batch in ["batch0", "batch1"]:
        for test_file in ["part-m-00000", "part-m-00001", "_SUCCESS"]:
            data_obj: storage.blob.Blob = gcs_bucket.blob("/".join([
                dest_dataset.dataset_id, dest_table.table_id, batch, test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nation",
                             test_file))
            data_objs.append(data_obj)

    def teardown():
        for do in data_objs:
            if do.exists():
                do.delete()

    request.addfinalizer(teardown)
    return [data_objs[-1], data_objs[-4]]


@pytest.fixture
def gcs_external_config(request, gcs_bucket, dest_dataset,
                        dest_table) -> List[storage.blob.Blob]:
    config_objs = []
    sql_obj = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}",
        dest_table.table_id,
        "_config",
        "bq_transform.sql",
    ]))

    sql = "INSERT {dest_dataset}.{dest_table} SELECT * FROM temp_ext"
    sql_obj.upload_from_string(sql)

    config_obj = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}",
        dest_table.table_id, "_config", "external.json"
    ]))

    with open(os.path.join(TEST_DIR, "resources",
                           "nation_schema.json")) as schema:
        fields = json.load(schema)
    config = {
        "schema": {
            "fields": fields
        },
        "csvOptions": {
            "allowJaggedRows": False,
            "allowQuotedNewlines": False,
            "encoding": "UTF-8",
            "fieldDelimiter": "|",
            "skipLeadingRows": 0,
        },
        "sourceFormat": "CSV",
        "sourceUris": ["REPLACEME"],
    }
    config_obj.upload_from_string(json.dumps(config))
    config_objs.append(sql_obj)
    config_objs.append(config_obj)

    def teardown():
        for do in config_objs:
            if do.exists():
                do.delete()

    request.addfinalizer(teardown)
    return config_objs


@pytest.fixture(scope="function")
def gcs_partitioned_data(request, gcs_bucket, dest_dataset,
                         dest_partitioned_table) -> List[storage.blob.Blob]:
    data_objs = []
    for partition in ["$2017041101", "$2017041102"]:
        for test_file in ["nyc_311.csv", "_SUCCESS"]:
            data_obj: storage.blob.Blob = gcs_bucket.blob("/".join([
                dest_dataset.dataset_id, dest_partitioned_table.table_id,
                partition, test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                             partition, test_file))
            data_objs.append(data_obj)

    def teardown():
        for dobj in data_objs:
            # we expect some backfill files to be removed by the cloud function.
            if dobj.exists():
                dobj.delete()

    request.addfinalizer(teardown)
    return [data_objs[-1], data_objs[-3]]


@pytest.fixture(scope="function")
def dest_partitioned_table(request, bq: bigquery.Client, mock_env,
                           dest_dataset) -> bigquery.Table:
    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))
    schema = public_table.schema

    table: bigquery.Table = bigquery.Table(
        f"{os.environ.get('GCP_PROJECT')}"
        f".{dest_dataset.dataset_id}.cf_test_nyc_311",
        schema=schema,
    )

    table.time_partitioning = bigquery.TimePartitioning()
    table.time_partitioning.type_ = bigquery.TimePartitioningType.HOUR
    table.time_partitioning.field = "created_date"

    table = bq.create_table(table)

    def teardown():
        bq.delete_table(table, not_found_ok=True)

    request.addfinalizer(teardown)
    return table


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


@pytest.fixture
def dest_ordered_update_table(request, bq, mock_env,
                              dest_dataset) -> bigquery.Table:
    with open(os.path.join(TEST_DIR, "resources",
                           "ordering_schema.json")) as schema_file:
        schema = gcs_ocn_bq_ingest.utils.dict_to_bq_schema(
            json.load(schema_file))

    table = bigquery.Table(
        f"{os.environ.get('GCP_PROJECT')}.{dest_dataset.dataset_id}"
        ".cf_test_ordering",
        schema=schema,
    )

    table = bq.create_table(table)
    # Our test query only updates so we need to populate the first row.
    bq.load_table_from_json([{"id": 1, "alpha_update": ""}], table)

    def teardown():
        bq.delete_table(table, not_found_ok=True)

    request.addfinalizer(teardown)
    return table


@pytest.fixture(scope="function")
def gcs_ordered_update_data(
        request, gcs_bucket, dest_dataset,
        dest_ordered_update_table) -> List[storage.blob.Blob]:
    data_objs = []
    chunks = {
        "00",
        "01",
        "02",
    }
    for chunk in chunks:
        for test_file in ["data.csv", "_SUCCESS"]:
            data_obj: storage.blob.Blob = gcs_bucket.blob("/".join([
                f"{dest_dataset.project}.{dest_dataset.dataset_id}",
                dest_ordered_update_table.table_id, chunk, test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "ordering",
                             chunk, test_file))
            data_objs.append(data_obj)

    def teardown():
        for dobj in data_objs:
            if dobj.exists():
                dobj.delete()

    request.addfinalizer(teardown)
    return list(filter(lambda do: do.name.endswith("_SUCCESS"), data_objs))


@pytest.fixture(scope="function")
def gcs_backlog(request, gcs, gcs_bucket,
                gcs_ordered_update_data) -> List[storage.blob.Blob]:
    data_objs = []

    for success_blob in gcs_ordered_update_data:
        gcs_ocn_bq_ingest.ordering.backlog_publisher(gcs, success_blob)
        backlog_blob = gcs_ocn_bq_ingest.ordering.success_blob_to_backlog_blob(
            success_blob)
        backlog_blob.upload_from_string("")
        data_objs.append(backlog_blob)

    def teardown():
        for dobj in data_objs:
            if dobj.exists():
                dobj.delete()

    request.addfinalizer(teardown)
    return list(filter(lambda do: do.name.endswith("_SUCCESS"), data_objs))


@pytest.fixture
def gcs_external_update_config(request, gcs_bucket, dest_dataset,
                               dest_ordered_update_table) -> storage.Blob:
    config_objs = []
    sql_obj = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}",
        dest_ordered_update_table.table_id,
        "_config",
        "bq_transform.sql",
    ]))

    sql = """
    UPDATE {dest_dataset}.{dest_table} dest
    SET alpha_update = CONCAT(dest.alpha_update, src.alpha_update)
    FROM temp_ext src
    WHERE dest.id = src.id
    """
    sql_obj.upload_from_string(sql)

    config_obj = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}",
        dest_ordered_update_table.table_id, "_config", "external.json"
    ]))

    with open(os.path.join(TEST_DIR, "resources",
                           "ordering_schema.json")) as schema:
        fields = json.load(schema)
    config = {
        "schema": {
            "fields": fields
        },
        "csvOptions": {
            "allowJaggedRows": False,
            "allowQuotedNewlines": False,
            "encoding": "UTF-8",
            "fieldDelimiter": "|",
            "skipLeadingRows": 0,
        },
        "sourceFormat": "CSV",
        "sourceUris": ["REPLACEME"],
    }
    config_obj.upload_from_string(json.dumps(config))
    backfill_blob = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}",
        dest_ordered_update_table.table_id,
        gcs_ocn_bq_ingest.constants.BACKFILL_FILENAME
    ]))
    backfill_blob.upload_from_string("")
    config_objs.append(sql_obj)
    config_objs.append(config_obj)
    config_objs.append(backfill_blob)

    def teardown():
        for do in config_objs:
            if do.exists():
                do.delete()

    request.addfinalizer(teardown)
    return backfill_blob


@pytest.mark.usefixtures("bq", "gcs_bucket", "dest_dataset",
                         "dest_partitioned_table")
@pytest.fixture
def gcs_external_partitioned_config(
        request, bq, gcs_bucket, dest_dataset,
        dest_partitioned_table) -> List[storage.blob.Blob]:
    config_objs = []
    sql_obj = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id,
        dest_partitioned_table.table_id,
        "_config",
        "bq_transform.sql",
    ]))

    sql = "INSERT {dest_dataset}.cf_test_nyc_311 SELECT * FROM temp_ext"
    sql_obj.upload_from_string(sql)

    config_obj = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id, dest_partitioned_table.table_id, "_config",
        "external.json"
    ]))

    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))
    config = {
        "schema": public_table.to_api_repr()['schema'],
        "csvOptions": {
            "allowJaggedRows": False,
            "allowQuotedNewlines": False,
            "encoding": "UTF-8",
            "fieldDelimiter": "|",
            "skipLeadingRows": 0,
        },
        "sourceFormat": "CSV",
        "sourceUris": ["REPLACEME"],
    }
    config_obj.upload_from_string(json.dumps(config))
    config_objs.append(sql_obj)
    config_objs.append(config_obj)

    def teardown():
        for do in config_objs:
            if do.exists:
                do.delete()

    request.addfinalizer(teardown)
    return config_objs
