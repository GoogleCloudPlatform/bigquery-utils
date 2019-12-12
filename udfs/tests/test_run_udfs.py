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

from parameterized import parameterized
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError

from utils import Utils


class TestRunUDFs(unittest.TestCase):

    @parameterized.expand(Utils.get_all_udf_paths())
    def test_run_udf_and_verify_expected_result(self, udf_path):
        client = bigquery.Client()
        bq_test_dataset = Utils.get_target_bq_dataset(udf_path)
        udf_name = Utils.extract_udf_name(udf_path)
        test_cases = Utils.load_test_cases(udf_path)
        if test_cases.get(udf_name):
            for case in test_cases[udf_name]:
                try:
                    actual_result_rows = client.query(
                        f'SELECT {bq_test_dataset}.{udf_name}('
                        f' {case["input"]} )'
                    ).result()

                    expected_result_rows = client.query(
                        f'SELECT {case["expected_output"]}'
                    ).result()

                    for actual, expected in zip(
                            actual_result_rows, expected_result_rows):
                        self.assertEqual(expected, actual)
                except GoogleAPICallError as e:
                    self.fail(e.message)
        else:
            self.skipTest(f'Test inputs and outputs are not provided for : {udf_path}')


if __name__ == '__main__':
    unittest.main()
