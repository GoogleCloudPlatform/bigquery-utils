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

import os
import unittest

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.bigquery.table import _EmptyRowIterator
from parameterized import parameterized

import udf_test_utils as utils


class TestCreateUDFs(unittest.TestCase):
    """
    This class uses the parameterized python package
    (https://pypi.org/project/parameterized/) to programmatically
    create multiple python test function definitions
    (based off `test_create_udf`). It will effectively create a python test
    function for each UDF that it encounters as it walks
    through the udfs/ directory.
    """

    @classmethod
    def setUpClass(cls):
        cls._client = bigquery.Client(project=os.getenv('BQ_PROJECT_ID'))

    @parameterized.expand(utils.get_all_udf_paths())
    def test_create_udf(self, udf_path):
        try:
            with open(udf_path) as udf_file:
                udf_sql = udf_file.read()
                udf_sql = utils.replace_with_js_bucket(os.getenv('_JS_BUCKET') or 'gs://bqutil-lib/bq_js_libs', udf_sql)
            # Only replace UDF datasets with a test dataset if the
            # build was triggered by a pull request or a non-main branch
            if (os.getenv('_PR_NUMBER') is not None or
                    os.getenv('BRANCH_NAME') != 'master'):
                udf_sql = utils.replace_with_test_datasets(
                    self._client.project, udf_sql)
                if udf_sql is None:
                    self.fail(f'Unable to replace SQL DDL with testing dataset '
                              f'for UDF: {udf_path}')
            udf_creation_result = self._client.query(udf_sql).result()

            self.assertIsInstance(udf_creation_result, _EmptyRowIterator)
        except GoogleAPICallError as error:
            self.fail(error.message)


if __name__ == '__main__':
    unittest.main()
