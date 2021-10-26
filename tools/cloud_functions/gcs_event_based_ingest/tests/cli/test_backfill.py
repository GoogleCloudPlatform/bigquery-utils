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
"""integrtion tests for gcs_ocn_bq_ingest"""
import os

import backfill
import pytest
from tests import utils as test_utils

TEST_DIR = os.path.realpath(os.path.dirname(__file__) + "/..")
LOAD_JOB_POLLING_TIMEOUT = 20  # seconds


@pytest.mark.IT
@pytest.mark.CLI
def test_backfill(bq, gcs_partitioned_data, gcs_truncating_load_config,
                  gcs_bucket, dest_partitioned_table):
    """
    This is an adaptation of test_load_job_partitioned but instead uses the
    backfill CLI code path to execute the cloud function's main method in
    parallel threads.

    Test loading separate partitions with WRITE_TRUNCATE

    after both load jobs the count should equal the sum of the test data in both
    partitions despite having WRITE_TRUNCATE disposition because the destination
    table should target only a particular partition with a decorator.
    """
    test_utils.check_blobs_exist(
        gcs_truncating_load_config,
        "the test is not configured correctly the load.json is missing")
    test_utils.check_blobs_exist(gcs_partitioned_data,
                                 "test data objects must exist")

    expected_num_rows = 0
    for part in [
            "$2017041101",
            "$2017041102",
    ]:
        test_data_file = os.path.join(TEST_DIR, "resources", "test-data",
                                      "nyc_311", part, "nyc_311.csv")
        expected_num_rows += sum(1 for _ in open(test_data_file))
    args = backfill.parse_args([
        f"--gcs-path=gs://{gcs_bucket.name}",
        "--mode=LOCAL",
    ])
    backfill.main(args)
    test_utils.bq_wait_for_rows(bq, dest_partitioned_table, expected_num_rows)
