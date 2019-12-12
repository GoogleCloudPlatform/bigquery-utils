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
from google.cloud.bigquery import QueryJobConfig
from google.cloud.bigquery.table import _EmptyRowIterator

from utils import Utils


class CreateUDFSignatures(unittest.TestCase):

    @parameterized.expand(Utils.get_all_udf_paths())
    def test_create_udf_signature(self, udf_path):
        client = bigquery.Client()
        bq_test_dataset = Utils.get_target_bq_dataset(udf_path)
        client.create_dataset(bq_test_dataset, exists_ok=True)

        job_config = QueryJobConfig()
        job_config.default_dataset = (
            f'{client.project}.{bq_test_dataset}'
        )
        udf_name = Utils.extract_udf_signature(udf_path)
        try:
            udf_creation_result = client.query(
                f'CREATE OR REPLACE FUNCTION {udf_name} AS (0)',
                job_config=job_config
            ).result()
            self.assertIsInstance(udf_creation_result, _EmptyRowIterator)
        except GoogleAPICallError as e:
            self.fail(e.message)


if __name__ == '__main__':
    unittest.main()
