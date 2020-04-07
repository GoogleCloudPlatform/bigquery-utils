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
from pathlib import Path

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from parameterized import parameterized

import udf_test_utils as utils


class TestRunUDFs(unittest.TestCase):
    """
    This class uses the parameterized python package (https://pypi.org/project/parameterized/) to programmatically
    create multiple python test function definitions (based off `test_run_udf_and_verify_expected_result`).
    It will effectively create a python test function for each UDF that it encounters as it walks through the
    udfs/ directory. This class tests each UDF by running it in BigQuery with the inputs given in the test_cases.yaml
    file, and then asserting that the results equal the expected outputs given in the test_cases.yaml file.
    """

    @classmethod
    def setUpClass(cls):
        cls._client = bigquery.Client()

    @parameterized.expand(utils.get_all_udf_paths())
    def test_run_udf_and_verify_expected_result(self, udf_path):
        client = self._client
        bq_test_dataset = utils.get_target_bq_dataset(udf_path)
        file_name = Path(udf_path).stem
        udf_name = utils.extract_udf_name(udf_path)
        self.assertEqual(udf_name, file_name,
                         msg=(f'\nFile name: {udf_path}'
                              f'\nshould match the function name defined inside: {udf_name}'))
        test_cases = utils.load_test_cases(udf_path)
        if test_cases.get(udf_name) is None:
            self.skipTest(f'Test inputs and outputs are not provided for : {udf_path}')
        for case in test_cases[udf_name]:
            try:
                actual_result_rows = client.query(
                    f'SELECT `{bq_test_dataset}.{udf_name}`('
                    f' {case["input"]} ) AS actual_result_rows'
                ).result()

                expected_result_rows = client.query(
                    f'SELECT ( {case["expected_output"]} ) AS expected_result_rows'
                ).result()

                for expected_result_row, actual_result_row in zip(expected_result_rows, actual_result_rows):
                    self.assertSequenceEqual(
                        expected_result_row.values(),
                        actual_result_row.values(),
                        msg=(f'\nTest failed for: {client.project}.{bq_test_dataset}.{udf_name}( {case["input"]} )'
                             f'\nExpected output: {expected_result_row.values()}'
                             f'\nActual output: {actual_result_row.values()}'))

            except GoogleAPICallError as e:
                self.fail(e.message)


if __name__ == '__main__':
    unittest.main()
