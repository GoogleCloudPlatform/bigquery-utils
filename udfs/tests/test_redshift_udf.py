# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from google.cloud import bigquery
from parameterized import parameterized

from udf_test import UDFTest

UDF_DIR = 'udfs/migration/redshift'
UDF_PATHS = UDFTest.get_udf_paths(UDF_DIR)
BIGQUERY_TEST_DATASET = 'rs_test'


class TestRedshiftUDF(UDFTest):

    @classmethod
    def setUpClass(cls):
        cls._client = bigquery.Client()
        cls._client.create_dataset(BIGQUERY_TEST_DATASET, exists_ok=True)
        cls._load_test_cases(cls, UDF_DIR)

    @unittest.skipUnless(UDF_PATHS, f'No UDFs found in {UDF_DIR}')
    @parameterized.expand(UDF_PATHS, skip_on_empty=True)
    def test_create_udf(self, udf_path):
        self._test_create_udf(udf_path, BIGQUERY_TEST_DATASET)

    @unittest.skipUnless(UDF_PATHS, f'No UDFs found in {UDF_DIR}')
    @parameterized.expand(UDF_PATHS, skip_on_empty=True)
    def test_run_udf_and_verify_expected_result(self, udf_path):
        self._test_run_udf_and_verify_expected_result(udf_path, BIGQUERY_TEST_DATASET)


if __name__ == '__main__':
    unittest.main()
