# Copyright 2021 Google LLC.
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
import re
import shlex
import subprocess
import uuid

import pytest
from google.cloud import bigquery
from google.cloud import storage

TEST_DIR = os.path.realpath(os.path.dirname(__file__))

ANSI_ESCAPE_PATTERN = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')


@pytest.fixture(scope="module")
def bq() -> bigquery.Client:
    """BigQuery Client"""
    return bigquery.Client(location="US")


@pytest.fixture(scope="module")
def gcs() -> storage.Client:
    """GCS Client"""
    return storage.Client()


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
    monkeypatch.setenv("FUNCTION_NAME", "e2e-test")
    monkeypatch.setenv("FUNCTION_TIMEOUT_SEC", "540")
    monkeypatch.setenv("BQ_PROJECT", gcs.project)


@pytest.fixture(scope='module')
def terraform_infra(request):

    def _escape(in_str):
        if in_str is not None:
            return ANSI_ESCAPE_PATTERN.sub('', in_str.decode('UTF-8'))
        return None

    def _run(cmd):
        result = subprocess.run(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT,
                                cwd=TEST_DIR)
        print(_escape(result.stdout))
        if result.returncode == 0:
            return
        raise subprocess.CalledProcessError(returncode=result.returncode,
                                            cmd=result.args,
                                            output=_escape(result.stdout),
                                            stderr=_escape(result.stderr))

    init = shlex.split("terraform init")
    apply = shlex.split("terraform apply -auto-approve")
    destroy = shlex.split("terraform destroy -auto-approve")

    _run(init)
    _run(apply)

    def teardown():
        _run(destroy)

    request.addfinalizer(teardown)
    with open(os.path.join(TEST_DIR, "terraform.tfstate")) as tf_state_file:
        return json.load(tf_state_file)


@pytest.fixture
def dest_dataset(request, bq, monkeypatch):
    random_dataset = (f"test_bq_ingest_gcf_"
                      f"{str(uuid.uuid4())[:8].replace('-','_')}")
    dataset = bigquery.Dataset(f"{os.getenv('TF_VAR_project_id', 'bqutil')}"
                               f".{random_dataset}")
    dataset.location = "US"
    bq.create_dataset(dataset)
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
