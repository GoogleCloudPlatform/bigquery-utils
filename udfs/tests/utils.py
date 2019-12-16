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

import glob
from os import path
from os.path import dirname
import re

from yaml import SafeLoader
from yaml import load

BIGQUERY_TEST_DATASET_MAPPINGS = {
    'netezza': 'nz',
    'oracle': 'or',
    'redshift': 'rs_test',
    'teradata': 'td_test',
    'vertica': 've',
    'community': 'fn_test',
}


class Utils(object):

    @staticmethod
    def get_udfs_parent_dir():
        return 'udfs/'

    @staticmethod
    def get_all_udf_paths():
        return glob.glob(Utils.get_udfs_parent_dir() + '/**/*.sql', recursive=True)

    @staticmethod
    def load_test_cases(udf_path):
        udf_dir = '/'.join(udf_path.split('/')[:-1])
        yaml_test_data_path = f'{udf_dir}/test_cases.yaml'
        if path.isfile(yaml_test_data_path):
            with open(yaml_test_data_path, 'r') as yaml_file:
                return load(yaml_file, Loader=SafeLoader)

    @staticmethod
    def extract_udf_name(udf_path):
        with open(udf_path) as udf_file:
            udf_sql = udf_file.read()
        udf_sql = udf_sql.replace('\n', ' ')
        pattern = re.compile(r'FUNCTION\s*`?(\w+.)?(\w+)`?\s*\(')
        match = pattern.search(udf_sql)
        if match:
            udf_name = match[2]
            return udf_name

    @staticmethod
    def extract_udf_signature(udf_path):
        with open(udf_path) as udf_file:
            udf_sql = udf_file.read()
        udf_sql = udf_sql.replace('\n', ' ')
        pattern = re.compile(r'FUNCTION\s+(`?.+?`?.*?\)).*?\s+AS')
        match = pattern.search(udf_sql)
        if match:
            udf_name = match[1]
            return udf_name
        else:
            return udf_path

    @staticmethod
    def replace_with_test_datasets(udf_path=None, project_id=None, udf_sql=None):
        if udf_path:
            with open(udf_path) as udf_file:
                udf_sql = udf_file.read()
        udf_length_before_replacement = len(udf_sql)
        udf_sql = re.sub(
            r'(\w+\.)?(?P<bq_dataset>\w+)(?P<udf_name>\.\w+)\(',
            f'`{project_id}.\\g<bq_dataset>_test\\g<udf_name>`(',
            udf_sql)
        if udf_length_before_replacement == len(udf_sql):
            return None
        else:
            return udf_sql

    @staticmethod
    def get_target_bq_dataset(udf_path):
        parent_dir_name = str(dirname(udf_path).split('/')[-1])
        return BIGQUERY_TEST_DATASET_MAPPINGS.get(parent_dir_name)
