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
"""Integration tests for gcs_ocn_bq_ingest"""
import json
import os
import uuid
from typing import List

import pytest
from google.cloud import bigquery
from google.cloud import error_reporting
from google.cloud import storage

import gcs_ocn_bq_ingest.common.ordering
import gcs_ocn_bq_ingest.common.utils

TEST_DIR = os.path.realpath(os.path.dirname(__file__))
LOAD_JOB_POLLING_TIMEOUT = 10  # seconds


@pytest.fixture(scope="package")
def bq() -> bigquery.Client:
    """BigQuery Client"""
    return bigquery.Client(location="US")


@pytest.fixture(scope="package")
def gcs() -> storage.Client:
    """GCS Client"""
    return storage.Client()


@pytest.fixture(scope="package")
def error() -> error_reporting.Client:
    """GCS Client"""
    return error_reporting.Client()


@pytest.fixture
def gcs_bucket(request, gcs: storage.Client) -> storage.Bucket:
    """GCS bucket for test artifacts"""
    bucket = gcs.create_bucket(f"test_gcs_ocn_bq_ingest_{str(uuid.uuid4())}")
    bucket.versioning_enabled = True
    bucket.patch()
    # overide default field delimiter at bucket level
    load_config_json = {
        "fieldDelimiter": "|",
    }
    load_json_blob: storage.Blob = bucket.blob("_config/load.json")
    load_json_blob.upload_from_string(json.dumps(load_config_json))

    def teardown():
        # Since bucket has object versioning enabled, you must
        # delete all versions of objects before you can delete the bucket.
        for blob in gcs.list_blobs(bucket, versions=True):
            blob.delete()
        bucket.delete(force=True)

    request.addfinalizer(teardown)
    return bucket


@pytest.fixture
def mock_env(gcs, monkeypatch):
    """
    environment variable mocks

    All tests use this fixture; it is specified in the
    pyest.ini file as:
      [pytest]
      usefixtures = mock_env
    For more information on module-wide fixtures, see:
    https://docs.pytest.org/en/stable/fixture.html#use-fixtures-in-classes-and-modules-with-usefixtures
    """
    # Infer project from the gcs client application default credentials.
    monkeypatch.setenv("GCP_PROJECT", gcs.project)
    monkeypatch.setenv("FUNCTION_NAME", "integration-test")
    monkeypatch.setenv("FUNCTION_TIMEOUT_SEC", "540")
    monkeypatch.setenv("BQ_PROJECT", gcs.project)


@pytest.fixture
def ordered_mock_env(monkeypatch):
    """environment variable mocks"""
    monkeypatch.setenv("ORDER_PER_TABLE", "TRUE")


@pytest.fixture
def dest_dataset(request, bq, monkeypatch):
    random_dataset = (f"test_bq_ingest_gcf_"
                      f"{str(uuid.uuid4())[:8].replace('-', '_')}")
    if os.getenv('GCP_PROJECT') is None:
        monkeypatch.setenv("GCP_PROJECT", bq.project)
    dataset = bigquery.Dataset(f"{os.getenv('GCP_PROJECT')}"
                               f".{random_dataset}")
    dataset.location = "US"
    bq.create_dataset(dataset)
    print(f"created dataset {dataset.dataset_id}")

    def teardown():
        bq.delete_dataset(dataset, delete_contents=True, not_found_ok=True)

    request.addfinalizer(teardown)
    return dataset


@pytest.fixture
def dest_table(monkeypatch, request, bq, dest_dataset) -> bigquery.Table:
    with open(os.path.join(TEST_DIR, "resources",
                           "nation_schema.json")) as schema_file:
        schema = gcs_ocn_bq_ingest.common.utils.dict_to_bq_schema(
            json.load(schema_file))
    if os.getenv('GCP_PROJECT') is None:
        monkeypatch.setenv("GCP_PROJECT", bq.project)
    table = bq.create_table(
        bigquery.Table(
            f"{os.getenv('GCP_PROJECT')}"
            f".{dest_dataset.dataset_id}.cf_test_nation_"
            f"{str(uuid.uuid4()).replace('-', '_')}",
            schema=schema,
        ))

    def teardown():
        bq.delete_table(table, not_found_ok=True)

    request.addfinalizer(teardown)
    return table


@pytest.fixture
def gcs_data(gcs_bucket, dest_dataset, dest_table) -> storage.Blob:
    data_objs: List[storage.Blob] = []
    for test_file in ["part-m-00000", "part-m-00001", "_SUCCESS"]:
        data_obj: storage.Blob = gcs_bucket.blob("/".join([
            f"{dest_dataset.project}.{dest_dataset.dataset_id}",
            dest_table.table_id, test_file
        ]))
        data_obj.upload_from_filename(
            os.path.join(TEST_DIR, "resources", "test-data", "nation",
                         test_file))
        data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def gcs_data_under_sub_dirs(gcs_bucket, dest_dataset,
                            dest_table) -> storage.Blob:
    data_objs = []
    for test_file in ["part-m-00000", "part-m-00001", "_SUCCESS"]:
        data_obj: storage.Blob = gcs_bucket.blob("/".join([
            f"{dest_dataset.project}.{dest_dataset.dataset_id}",
            dest_table.table_id, "foo", "bar", "baz", test_file
        ]))
        data_obj.upload_from_filename(
            os.path.join(TEST_DIR, "resources", "test-data", "nation",
                         test_file))
        data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def gcs_truncating_load_config(gcs_bucket, dest_dataset,
                               dest_table) -> List[storage.Blob]:
    config_objs: List[storage.Blob] = []
    config_obj: storage.Blob = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id,
        dest_table.table_id,
        "_config",
        "load.json",
    ]))
    config_obj.upload_from_string(
        json.dumps({"writeDisposition": "WRITE_TRUNCATE"}))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_batched_data(gcs_bucket, dest_dataset,
                     dest_table) -> List[storage.Blob]:
    """
  upload two batches of data
  """
    data_objs: List[storage.Blob] = []
    for batch in ["batch0", "batch1"]:
        for test_file in ["part-m-00000", "part-m-00001", "_SUCCESS"]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join([
                dest_dataset.dataset_id, dest_table.table_id, batch, test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nation",
                             test_file))
            data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def gcs_external_config(gcs_bucket, dest_dataset,
                        dest_table) -> List[storage.Blob]:
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
    return config_objs


@pytest.fixture
def gcs_destination_config(gcs_bucket, dest_dataset,
                           dest_partitioned_table) -> List[storage.Blob]:
    """
    This tests that a load.json file with destinationTable specified is used
    to load data.
    """
    config_objs = []
    config_obj: storage.Blob = gcs_bucket.blob("/".join([
        "_config",
        "load.json",
    ]))
    config_obj.upload_from_string(
        json.dumps({
            "writeDisposition":
                "WRITE_TRUNCATE",
            "fieldDelimiter":
                "|",
            "destinationTable": {
                "projectId": dest_partitioned_table.project,
                "datasetId": dest_partitioned_table.dataset_id,
                "tableId": dest_partitioned_table.table_id
            },
            "destinationRegex": (
                r"(?P<table>.*?)/"  # ignore everything leading up to partition
                r"\$?(?P<yyyy>[\d]{4})/?"  # partition year (yyyy) (optional)
                r"(?P<mm>[\d]{2})?/?"  # partition month (mm) (optional)
                r"(?P<dd>[\d]{2})?/?"  # partition day (dd)  (optional)
                r"(?P<hh>[\d]{2})?/?"  # partition hour (hh) (optional)
            )
        }))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_destination_parquet_config(
        gcs_bucket, dest_dataset, dest_partitioned_table) -> List[storage.Blob]:
    """
    This tests that a load.json file with destinationTable specified is used
    to load data.

    :param gcs_bucket:
    :param dest_dataset:
    :param dest_partitioned_table:
    :return:
    """
    destination_regex = (
        r"(?P<table>.*?)"  # ignore everything leading up to partition
        r"(?:[\d]{4})?/?"
        r"(?:[\d]{2})?/?"
        r"(?:[\d]{2})?/?"
        r"(?P<batch>[\d]{2})/?"  # batch
    )
    config_objs = []
    config_obj: storage.Blob = gcs_bucket.blob("/".join([
        "_config",
        "load.json",
    ]))
    config_obj.upload_from_string(
        json.dumps({
            "sourceFormat": "PARQUET",
            "destinationTable": {
                "projectId": dest_partitioned_table.project,
                "datasetId": dest_partitioned_table.dataset_id,
                "tableId": dest_partitioned_table.table_id
            },
            "destinationRegex": destination_regex,
        }))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_destination_parquet_config_hive_partitioned(
        gcs_bucket, dest_dataset,
        dest_hive_partitioned_table) -> List[storage.Blob]:
    """
    This tests that a load.json file with destinationTable and destinationRegex
    specified is used to load data.

    :param gcs_bucket:
    :param dest_dataset:
    :param dest_hive_partitioned_table:
    :return:
    """
    destination_regex = (
        r"(?P<table>.*?)/"  # ignore everything leading up to partition
        r"(?P<yyyy>[\d]{4})/"
        r"(?P<mm>[\d]{2})/"
        r"(?P<dd>[\d]{2})/"
        r"(?P<hh>[\d]{2})/"
        # r"^(?:[\w\-_0-9]+)/(?P<dataset>[\w\-_0-9\.]+)/"
        # r"(?P<table>[\w\-_0-9]+)/?"
        # r"(?:incremental|history)?/?"
        # r"(?:[0-9]{4})?/?"
        # r"(?:[0-9]{2})?/?"
        # r"(?:[0-9]{2})?/?"
        # r"(?:[0-9]{2})?/?"
        # r"(?P<batch>[0-9]+)/?"
    )
    config_objs = []
    config_obj: storage.Blob = gcs_bucket.blob("/".join([
        "_config",
        "load.json",
    ]))
    config_obj.upload_from_string(
        json.dumps({
            "sourceFormat": "PARQUET",
            "destinationTable": {
                "projectId": dest_hive_partitioned_table.project,
                "datasetId": dest_hive_partitioned_table.dataset_id,
                "tableId": dest_hive_partitioned_table.table_id
            },
            "destinationRegex": destination_regex,
            "dataSourceName": "some-onprem-data-source"
        }))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_destination_parquet_config_partitioned_alternate(
        gcs_bucket, dest_dataset, dest_partitioned_table) -> List[storage.Blob]:
    """
    This tests that a load.json file with destinationTable and destinationRegex
    specified is used to load data.

    :param gcs_bucket:
    :param dest_dataset:
    :param dest_hive_partitioned_table:
    :return:
    """
    destination_regex = (
        r"(?P<table>.*?)/"  # ignore everything leading up to partition
        r"year=(?P<yyyy>[\d]{4})/"
        r"month=(?P<mm>[\d]{1,2})/"
        r"day=(?P<dd>[\d]{1,2})/"
        r"hr=(?P<hh>[\d]{1,2})/")
    config_objs = []
    config_obj: storage.Blob = gcs_bucket.blob("/".join([
        "_config",
        "load.json",
    ]))
    config_obj.upload_from_string(
        json.dumps({
            "sourceFormat": "PARQUET",
            "destinationTable": {
                "projectId": dest_partitioned_table.project,
                "datasetId": dest_partitioned_table.dataset_id,
                "tableId": dest_partitioned_table.table_id
            },
            "destinationRegex": destination_regex,
            "dataSourceName": "some-onprem-data-source"
        }))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_partitioned_data(gcs_bucket, dest_dataset,
                         dest_partitioned_table) -> List[storage.Blob]:
    data_objs = []
    for partition in ["$2017041101", "$2017041102"]:
        for test_file in ["nyc_311.csv", "_SUCCESS"]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join([
                dest_dataset.dataset_id, dest_partitioned_table.table_id,
                partition, test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                             partition, test_file))
            data_objs.append(data_obj)
        dot_blob: storage.Blob = gcs_bucket.blob("/".join([
            dest_dataset.dataset_id, dest_partitioned_table.table_id, partition,
            ".file_that_starts_with_dot"
        ]))
        dot_blob.upload_from_string("")
        data_objs.append(dot_blob)
    return data_objs


@pytest.fixture
def gcs_partitioned_data_allow_jagged(
        gcs_bucket, dest_dataset,
        dest_partitioned_table_allow_jagged) -> List[storage.Blob]:
    data_objs = []
    for partition in ["$2017041101", "$2017041102"]:
        for test_file in ["nyc_311.csv.gz", "_SUCCESS"]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join([
                dest_dataset.dataset_id,
                dest_partitioned_table_allow_jagged.table_id, partition,
                test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                             partition, test_file))
            data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def gcs_partitioned_parquet_data(gcs_bucket, dest_dataset,
                                 dest_partitioned_table) -> List[storage.Blob]:
    data_objs = []
    for partition in ["$2017041101", "$2017041102"]:
        for test_file in [
                "nyc311_25_rows_00.parquet", "nyc311_25_rows_01.parquet",
                "_SUCCESS"
        ]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join(
                [partition, test_file]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                             partition, test_file))
            data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def gcs_split_path_partitioned_data(
        gcs_bucket, dest_dataset, dest_partitioned_table) -> List[storage.Blob]:
    data_objs = []
    for partition in ["$2017041101", "$2017041102"]:
        for test_file in ["nyc_311.csv", "_SUCCESS"]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join([
                "foo",
                "bar",
                "baz",
                partition[1:5],  # year
                partition[5:7],  # month
                partition[7:9],  # day
                partition[9:],  # hour
                "hive_part_column=9999",
                test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                             partition, test_file))
            data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def gcs_split_path_partitioned_parquet_data(
        gcs_bucket, dest_dataset, dest_partitioned_table) -> List[storage.Blob]:
    data_objs = []
    for partition in ["$2017041101", "$2017041102"]:
        for test_file in [
                "nyc311_25_rows_00.parquet", "nyc311_25_rows_01.parquet"
        ]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join([
                "foo",
                "bar",
                "baz",
                partition[1:5],  # year
                partition[5:7],  # month
                partition[7:9],  # day
                partition[9:],  # batch
                "hive_part_column=9999",
                test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                             partition, test_file))
            data_objs.append(data_obj)
        # Add _SUCCESS file under the hour partition folder
        data_obj = gcs_bucket.blob("/".join([
            "foo",
            "bar",
            "baz",
            partition[1:5],  # year
            partition[5:7],  # month
            partition[7:9],  # day
            partition[9:],  # batch
            "_SUCCESS"
        ]))
        data_obj.upload_from_filename(
            os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                         partition, "_SUCCESS"))
        data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def gcs_split_path_partitioned_parquet_data_alternate(
        gcs_bucket, dest_dataset, dest_partitioned_table) -> List[storage.Blob]:
    data_objs = []
    for partition in ["$2017041101", "$2017041102"]:
        for test_file in [
                "nyc311_25_rows_00.parquet", "nyc311_25_rows_01.parquet"
        ]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join([
                "foo",
                "bar",
                "baz",
                f"year={partition[1:5]}",  # year
                f"month={partition[5:7]}",  # month
                f"day={partition[7:9]}",  # day
                f"hr={partition[9:]}",  # batch
                test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                             partition, test_file))
            data_objs.append(data_obj)
        # Add _SUCCESS file under the hour partition folder
        data_obj = gcs_bucket.blob("/".join([
            "foo",
            "bar",
            "baz",
            f"year={partition[1:5]}",  # year
            f"month={partition[5:7]}",  # month
            f"day={partition[7:9]}",  # day
            f"hr={partition[9:]}",  # batch
            "_SUCCESS"
        ]))
        data_obj.upload_from_filename(
            os.path.join(TEST_DIR, "resources", "test-data", "nyc_311",
                         partition, "_SUCCESS"))
        data_objs.append(data_obj)
    return data_objs


@pytest.fixture
def dest_partitioned_table(bq: bigquery.Client, dest_dataset,
                           monkeypatch) -> bigquery.Table:
    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))
    schema = public_table.schema

    if os.getenv('GCP_PROJECT') is None:
        monkeypatch.setenv("GCP_PROJECT", bq.project)

    table: bigquery.Table = bigquery.Table(
        f"{os.getenv('GCP_PROJECT')}"
        f".{dest_dataset.dataset_id}.cf_test_nyc_311_"
        f"{str(uuid.uuid4()).replace('-', '_')}",
        schema=schema,
    )

    table.time_partitioning = bigquery.TimePartitioning()
    table.time_partitioning.type_ = bigquery.TimePartitioningType.HOUR
    table.time_partitioning.field = "created_date"

    table = bq.create_table(table)
    return table


@pytest.fixture
def dest_hive_partitioned_table(bq: bigquery.Client, dest_dataset,
                                monkeypatch) -> bigquery.Table:
    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))

    schema = public_table.schema
    schema.append(bigquery.SchemaField('hive_part_column', 'INT64'))

    if os.getenv('GCP_PROJECT') is None:
        monkeypatch.setenv("GCP_PROJECT", bq.project)

    table: bigquery.Table = bigquery.Table(
        f"{os.getenv('GCP_PROJECT')}"
        f".{dest_dataset.dataset_id}.cf_test_nyc_311_"
        f"{str(uuid.uuid4()).replace('-', '_')}",
        schema=schema,
    )

    table.time_partitioning = bigquery.TimePartitioning()
    table.time_partitioning.type_ = bigquery.TimePartitioningType.HOUR
    table.time_partitioning.field = "created_date"

    table = bq.create_table(table)
    return table


@pytest.fixture
def dest_partitioned_table_allow_jagged(bq: bigquery.Client, dest_dataset,
                                        monkeypatch) -> bigquery.Table:
    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))
    schema = public_table.schema

    if os.getenv('GCP_PROJECT') is None:
        monkeypatch.setenv("GCP_PROJECT", bq.project)

    extra_field_for_jagged_row_test = bigquery.schema.SchemaField(
        "extra_jagged_row_test_column", "STRING")
    schema.append(extra_field_for_jagged_row_test)
    table: bigquery.Table = bigquery.Table(
        f"{os.getenv('GCP_PROJECT')}"
        f".{dest_dataset.dataset_id}.cf_test_nyc_311_"
        f"{str(uuid.uuid4()).replace('-', '_')}",
        schema=schema,
    )

    table.time_partitioning = bigquery.TimePartitioning()
    table.time_partitioning.type_ = bigquery.TimePartitioningType.HOUR
    table.time_partitioning.field = "created_date"

    table = bq.create_table(table)
    return table


@pytest.fixture
def dest_ordered_update_table(gcs, gcs_bucket, bq,
                              dest_dataset) -> bigquery.Table:
    with open(os.path.join(TEST_DIR, "resources",
                           "ordering_schema.json")) as schema_file:
        schema = gcs_ocn_bq_ingest.common.utils.dict_to_bq_schema(
            json.load(schema_file))

    table: bigquery.Table = bigquery.Table(
        f"{dest_dataset.project}.{dest_dataset.dataset_id}"
        f".cf_test_ordering_{str(uuid.uuid4()).replace('-', '_')}",
        schema=schema,
    )

    table = bq.create_table(table)

    # Our test query only updates on a single row so we need to populate
    # original row.
    # This can be used to simulate an existing _bqlock from a prior run of the
    # subscriber loop with a job that has succeeded.
    job: bigquery.LoadJob = bq.load_table_from_json(
        [{
            "id": 1,
            "alpha_update": ""
        }],
        table,
        job_id_prefix=gcs_ocn_bq_ingest.common.constants.DEFAULT_JOB_PREFIX)

    # The subscriber will be responsible for cleaning up this file.
    bqlock_obj: storage.Blob = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}", table.table_id,
        "_bqlock"
    ]))

    bqlock_obj.upload_from_string(
        json.dumps(dict(job_id=job.job_id,
                        table=table.reference.to_api_repr())))
    return table


@pytest.fixture
def gcs_ordered_update_data(gcs_bucket, dest_dataset,
                            dest_ordered_update_table) -> List[storage.Blob]:
    data_objs = []
    older_success_blob: storage.Blob = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}",
        dest_ordered_update_table.table_id, "00", "_SUCCESS"
    ]))
    older_success_blob.upload_from_string("")
    data_objs.append(older_success_blob)

    chunks = {
        "01",
        "02",
        "03",
    }
    for chunk in chunks:
        for test_file in ["data.csv", "_SUCCESS"]:
            data_obj: storage.Blob = gcs_bucket.blob("/".join([
                f"{dest_dataset.project}.{dest_dataset.dataset_id}",
                dest_ordered_update_table.table_id, chunk, test_file
            ]))
            data_obj.upload_from_filename(
                os.path.join(TEST_DIR, "resources", "test-data", "ordering",
                             chunk, test_file))
            data_objs.append(data_obj)
    return list(filter(lambda do: do.name.endswith("_SUCCESS"), data_objs))


@pytest.fixture
def gcs_backlog(gcs, gcs_bucket, gcs_ordered_update_data) -> List[storage.Blob]:
    data_objs = []

    # We will deal with the last incremental in the test itself to test the
    # behavior of a new backlog subscriber.
    for success_blob in gcs_ordered_update_data:
        gcs_ocn_bq_ingest.common.ordering.backlog_publisher(gcs, success_blob)
        backlog_blob = gcs_ocn_bq_ingest.common.ordering.success_blob_to_backlog_blob(
            gcs, success_blob)
        backlog_blob.upload_from_string("")
        data_objs.append(backlog_blob)
    return list(filter(lambda do: do.name.endswith("_SUCCESS"), data_objs))


@pytest.fixture
def gcs_external_update_config(gcs_bucket, dest_dataset,
                               dest_ordered_update_table) -> List[storage.Blob]:
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
        gcs_ocn_bq_ingest.common.constants.BACKFILL_FILENAME
    ]))
    backfill_blob.upload_from_string("")
    config_objs.append(sql_obj)
    config_objs.append(config_obj)
    config_objs.append(backfill_blob)
    return config_objs


@pytest.fixture
def gcs_external_partitioned_config(
        bq, gcs_bucket, dest_dataset,
        dest_partitioned_table) -> List[storage.Blob]:
    config_objs = []
    sql_obj = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id,
        dest_partitioned_table.table_id,
        "_config",
        "bq_transform.sql",
    ]))
    sql = "INSERT {dest_dataset}.{dest_table} SELECT * FROM temp_ext;"
    sql_obj.upload_from_string(sql)
    config_objs.append(sql_obj)

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
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_external_partitioned_config_allow_jagged(
        bq, gcs_bucket, dest_dataset,
        dest_partitioned_table_allow_jagged) -> List[storage.Blob]:
    config_objs = []
    sql_obj = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id,
        dest_partitioned_table_allow_jagged.table_id,
        "_config",
        "bq_transform.sql",
    ]))
    sql = "INSERT {dest_dataset}.{dest_table} SELECT * FROM temp_ext;"
    sql_obj.upload_from_string(sql)
    config_objs.append(sql_obj)

    config_obj = gcs_bucket.blob("/".join([
        dest_dataset.dataset_id, dest_partitioned_table_allow_jagged.table_id,
        "_config", "external.json"
    ]))
    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))

    extra_field_for_jagged_row_test = bigquery.schema.SchemaField(
        "extra_jagged_row_test_column", "STRING")
    jagged_schema = public_table.schema + [extra_field_for_jagged_row_test]
    config = {
        "schema": {
            "fields": [
                schema_field.to_api_repr() for schema_field in jagged_schema
            ]
        },
        "compression": "GZIP",
        "csvOptions": {
            "allowJaggedRows": True,
            "allowQuotedNewlines": False,
            "encoding": "UTF-8",
            "fieldDelimiter": "|",
            "skipLeadingRows": 0,
        },
        "sourceFormat": "CSV",
        "sourceUris": ["REPLACEME"],
    }
    config_obj.upload_from_string(json.dumps(config))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_external_partitioned_parquet_config(
        bq, gcs_bucket, dest_dataset,
        dest_partitioned_table) -> List[storage.Blob]:
    config_objs = []
    # Upload SQL query used to load table
    sql_obj = gcs_bucket.blob("/".join([
        "_config",
        "bq_transform.sql",
    ]))
    sql_obj.upload_from_string("INSERT {dest_dataset}.{dest_table} "
                               "SELECT * FROM temp_ext;")
    config_objs.append(sql_obj)
    # Upload external table definition
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externaldataconfiguration
    config_obj = gcs_bucket.blob("/".join(["_config", "external.json"]))
    config_obj.upload_from_string(json.dumps({"sourceFormat": "PARQUET"}))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def gcs_external_hive_partitioned_parquet_config(
        bq, gcs_bucket, dest_dataset,
        dest_partitioned_table) -> List[storage.Blob]:
    config_objs = []
    # Upload SQL query used to load table
    sql_obj = gcs_bucket.blob("/".join([
        "_config",
        "bq_transform.sql",
    ]))
    sql_obj.upload_from_string("INSERT {dest_dataset}.{dest_table} "
                               "SELECT * FROM temp_ext;")
    config_objs.append(sql_obj)
    # Upload external table definition
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#externaldataconfiguration
    config_obj = gcs_bucket.blob("/".join(["_config", "external.json"]))
    config_obj.upload_from_string(
        json.dumps({
            "sourceFormat": "PARQUET",
            "hivePartitioningOptions": {
                "mode": "AUTO"
            }
        }))
    config_objs.append(config_obj)
    return config_objs


@pytest.fixture
def no_use_error_reporting(monkeypatch):
    monkeypatch.setenv("USE_ERROR_REPORTING_API", "False")


@pytest.fixture
def gcs_external_config_bad_statement(
        gcs_bucket, dest_dataset, dest_table,
        no_use_error_reporting) -> List[storage.Blob]:
    config_objs = []
    sql_obj = gcs_bucket.blob("/".join([
        f"{dest_dataset.project}.{dest_dataset.dataset_id}",
        dest_table.table_id,
        "_config",
        "bq_transform.sql",
    ]))

    sql = ("Woops this isn't valid SQL;\n"
           "INSERT {dest_dataset}.{dest_table} SELECT * FROM temp_ext;")
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
    return config_objs
