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
"""End-to-end tests for event based BigQuery ingest Cloud Function."""
import json
import os
import uuid

import pytest
from google.cloud import bigquery
from google.cloud import storage


def pytest_addoption(parser):
    # if Terraform was used to deploy resources, pass the state details
    parser.addoption("--tfstate", action="store", default=None)


@pytest.fixture(scope="module")
def bq() -> bigquery.Client:
    """BigQuery Client"""
    return bigquery.Client(location="US")


@pytest.fixture(scope="module")
def gcs() -> storage.Client:
    """GCS Client"""
    return storage.Client()


@pytest.fixture(scope='module')
def tf_state(pytestconfig):

    # if we used Terraform to create the GCP resources, use the output variables
    if pytestconfig.getoption('tfstate') is not None:
        tf_state_file = pytestconfig.getoption('tfstate')
        with open(tf_state_file, 'r', encoding='utf-8') as fp:
            return json.load(fp)


@pytest.fixture
def dest_dataset(request, bq, monkeypatch):
    random_dataset = (f"test_bq_ingest_gcf_"
                      f"{str(uuid.uuid4())[:8].replace('-','_')}")
    dataset = bigquery.Dataset(f"{os.getenv('TF_VAR_project_id', 'bqutil')}"
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


@pytest.fixture(scope="function")
def dest_table(request, bq: bigquery.Client, dest_dataset) -> bigquery.Table:
    public_table: bigquery.Table = bq.get_table(
        bigquery.TableReference.from_string(
            "bigquery-public-data.new_york_311.311_service_requests"))
    schema = public_table.schema

    table: bigquery.Table = bigquery.Table(
        f"{os.environ.get('TF_VAR_project_id', 'bqutil')}"
        f".{dest_dataset.dataset_id}.cf_e2e_test_nyc_311_"
        f"{os.getenv('SHORT_SHA', 'manual')}",
        schema=schema,
    )

    table = bq.create_table(table)

    def teardown():
        bq.delete_table(table, not_found_ok=True)

    request.addfinalizer(teardown)
    return table
