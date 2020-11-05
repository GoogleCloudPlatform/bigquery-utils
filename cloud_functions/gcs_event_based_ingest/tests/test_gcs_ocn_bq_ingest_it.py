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
import json
import os
import sys
import uuid
from time import monotonic
from typing import List

import google.cloud.storage as storage
import pytest
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
from gcs_ocn_bq_ingest import main

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
LOAD_JOB_POLLING_TIMEOUT = 10    # seconds


@pytest.fixture(scope="module")
def bq() -> bigquery.Client:
    """BigQuery Client"""
    return bigquery.Client(location="US")


@pytest.fixture(scope="module")
def gcs() -> storage.Client:
    """GCS Client"""
    return storage.Client()


@pytest.mark.usefixtures("gcs")
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


@pytest.mark.usefixtures("gcs_bucket")
@pytest.fixture
def mock_env(gcs, monkeypatch):
    """environment variable mocks"""
    # Infer project from ADC of gcs client.
    monkeypatch.setenv("GCP_PROJECT", gcs.project)
    monkeypatch.setenv("FUNCTION_NAME", "integration-test")


@pytest.mark.usefixtures("bq", "mock_env")
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


@pytest.mark.usefixtures("bq", "mock_env", "dest_dataset")
@pytest.fixture
def dest_table(request, bq, mock_env, dest_dataset) -> bigquery.Table:
    with open(os.path.join(TEST_DIR, "resources",
                           "nation_schema.json")) as schema_file:
        schema = main.dict_to_bq_schema(json.load(schema_file))

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
@pytest.mark.usefixtures("gcs_bucket", "dest_dataset", "dest_table")
def gcs_data(request, gcs_bucket, dest_dataset,
             dest_table) -> storage.blob.Blob:
    data_objs = []
    for test_file in ["part-m-00000", "part-m-00001", "_SUCCESS"]:
        data_obj: storage.blob.Blob = gcs_bucket.blob("/".join(
            [dest_dataset.dataset_id, dest_table.table_id, test_file]))
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
    main.main(test_event, None)
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
    main.main(test_event, None)
    did_second_invocation_raise = False
    try:
        main.main(test_event, None)
    except RuntimeError:
        did_second_invocation_raise = True
    assert did_second_invocation_raise

    test_data_file = os.path.join(TEST_DIR, "resources", "test-data", "nation",
                                  "part-m-00001")
    expected_num_rows = sum(1 for _ in open(test_data_file))
    bq_wait_for_rows(bq, dest_table, expected_num_rows)


@pytest.fixture(scope="function")
@pytest.mark.usefixtures("gcs_bucket", "dest_dataset", "dest_table")
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
@pytest.mark.usefixtures("gcs_bucket", "dest_dataset", "dest_table")
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
            if do.exists:
                do.delete()

    request.addfinalizer(teardown)
    return [data_objs[-1], data_objs[-4]]


@pytest.mark.IT
def test_load_job_truncating_batches(bq, gcs_batched_data,
                                        gcs_truncating_load_config,
                                        dest_dataset, dest_table, mock_env):
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
        main.main(test_event, None)
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
        main.main(test_event, None)
        bq_wait_for_rows(bq, dest_table, expected_counts[i])


@pytest.mark.usefixtures("gcs_bucket", "dest_dataset", "dest_table")
@pytest.fixture
def gcs_external_config(request, gcs_bucket, dest_dataset,
                        dest_table) -> List[storage.blob.Blob]:
    config_objs = []
    sql_obj = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id,
        dest_table.table_id,
        "_config",
        "bq_transform.sql",
    ]))

    sql = "INSERT {dest_dataset}.{dest_table} SELECT * FROM temp_ext"
    sql_obj.upload_from_string(sql)

    config_obj = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id, dest_table.table_id, "_config",
        "external.json"
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
            if do.exists:
                do.delete()

    request.addfinalizer(teardown)
    return config_objs


@pytest.mark.IT
def test_external_query(bq, gcs_data, gcs_external_config, dest_dataset,
                           dest_table, mock_env):
    """tests the basic external query ingrestion mechanics
    with bq_transform.sql and external.json
    """
    if not gcs_data.exists():
        raise NotFound("test data objects must exist")
    if not all((blob.exists() for blob in gcs_external_config)):
        raise NotFound("config objects must exist")

    test_event = {
        "attributes": {
            "bucketId": gcs_data.bucket.name,
            "objectId": gcs_data.name
        }
    }
    main.main(test_event, None)
    test_data_file = os.path.join(TEST_DIR, "resources", "test-data", "nation",
                                  "part-m-00001")
    expected_num_rows = sum(1 for _ in open(test_data_file))
    bq_wait_for_rows(bq, dest_table, expected_num_rows)


@pytest.fixture(scope="function")
@pytest.mark.usefixtures("gcs_bucket", "dest_dataset", "dest_parttioned_table")
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
            if dobj.exists:
                dobj.delete()

    request.addfinalizer(teardown)
    return [data_objs[-1], data_objs[-3]]


@pytest.fixture(scope="function")
@pytest.mark.usefixtures("gcs_bucket", "dest_dataset", "dest_table")
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
        main.main(test_event, None)
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

    start_poll = monotonic()
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
        f"{table.project}.{table.dataset_id}.{table.table_id} to"
        f"reach {expected_num_rows} rows."
        f"last poll returned {actual_num_rows} rows.")
