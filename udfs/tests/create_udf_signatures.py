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


class CreateUDFSignatures(unittest.TestCase):
    """
    This class uses the parameterized python package
    (https://pypi.org/project/parameterized/) to programmatically
    create multiple python test function definitions
    (based off `test_create_udf_signature`). It will effectively create a python
    test function for each UDF that it encounters as it walks through
    the udfs/ directory.

    This class creates all UDFs with NULL bodies in order to prevent errors when
    subsequently creating all UDFs concurrently in the TestCreateUDFs class.
    Some UDFs may invoke other UDFs within their body and therefore have a
    dependency on them which could lead to errors upon creation:
    (e.g. one UDF might not have been created yet but is needed by another UDF
     currently being created)
    """

    @classmethod
    def setUpClass(cls):
        cls._client = bigquery.Client(project=os.getenv('BQ_PROJECT_ID'))

    @parameterized.expand(utils.get_all_udf_paths())
    def test_create_udf_signature(self, udf_path):
        client = self._client
        udf_signature = utils.replace_with_null_body(udf_path)
        udf_signature = utils.replace_with_test_datasets(
            client.project, udf_signature)
        try:
            udf_creation_result = client.query(udf_signature).result()
            self.assertIsInstance(udf_creation_result, _EmptyRowIterator)
        except GoogleAPICallError as error:
            self.fail(error.message)


if __name__ == '__main__':
    unittest.main()
