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

from os import listdir
from os import path
import re
import unittest

from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigquery import QueryJobConfig
from google.cloud.bigquery.table import _EmptyRowIterator
from yaml import SafeLoader
from yaml import load


class UDFTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Client object will be initialized in child class
        cls._client = None
        cls._test_cases = None

    def _load_test_cases(self, udf_dir):
        yaml_test_data_path = f'{udf_dir}/test_cases.yaml'
        if path.isfile(yaml_test_data_path):
            with open(yaml_test_data_path, 'r') as yaml_file:
                self._test_cases = load(yaml_file, Loader=SafeLoader)

    def _test_create_udf(self, udf_path, dataset):
        job_config = QueryJobConfig()
        job_config.default_dataset = (
            f'{self._client.project}.{dataset}'
        )
        with open(udf_path) as udf_file:
            try:
                udf_creation_result = self._client.query(
                    udf_file.read(),
                    job_config=job_config
                ).result()
                self.assertIsInstance(udf_creation_result, _EmptyRowIterator)
            except GoogleAPICallError as e:
                self.fail(e.message)

    def _test_run_udf_and_verify_expected_result(self, udf_path, dataset):
        udf_name = self.extract_udf_name(udf_path)
        if self._test_cases.get(udf_name):
            for case in self._test_cases[udf_name]:
                try:
                    actual_result_rows = self._client.query(
                        f'SELECT {dataset}.{udf_name}('
                        f' {case["input"]} )'
                    ).result()

                    expected_result_rows = self._client.query(
                        f'SELECT {case["expected_output"]}'
                    ).result()

                    for actual, expected in zip(
                            actual_result_rows, expected_result_rows):
                        self.assertEqual(expected, actual)
                except GoogleAPICallError as e:
                    self.fail(e.message)
        else:
            self.skipTest('Test inputs and outputs are not available')

    @staticmethod
    def extract_udf_name(udf_path):
        with open(udf_path) as udf_file:
            udf_sql = udf_file.read()
        pattern = re.compile(r'FUNCTION\s*`?(\w+)`?\s*\(')
        match = pattern.search(udf_sql)
        if match:
            udf_name = match[1]
            return udf_name

    @staticmethod
    def get_udf_paths(udf_dir_path):
        return [path.join(udf_dir_path, f)
                for f in listdir(udf_dir_path) if f.endswith('.sql')]


if __name__ == '__main__':
    unittest.main()
