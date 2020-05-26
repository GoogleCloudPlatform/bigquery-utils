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

import argparse
import glob

import os
from pathlib import Path
import re

from yaml import load
from yaml import SafeLoader

from google.cloud import bigquery

BIGQUERY_TEST_DATASET_MAPPINGS = {
    'netezza': 'nz_test',
    'oracle': 'or_test',
    'redshift': 'rs_test',
    'teradata': 'td_test',
    'vertica': 've_test',
    'community': 'fn_test',
}

UDF_PARENT_DIR = 'udfs/'


def get_all_udf_paths():
    return glob.glob(UDF_PARENT_DIR + '/**/*.sql', recursive=True)


def load_test_cases(udf_path):
    udf_dir = Path(udf_path).parent
    yaml_test_data_path = udf_dir / 'test_cases.yaml'
    if yaml_test_data_path.is_file():
        with open(yaml_test_data_path, 'r') as yaml_file:
            return load(yaml_file, Loader=SafeLoader)
    else:
        return None


def extract_udf_name(udf_path):
    with open(udf_path) as udf_file:
        udf_sql = udf_file.read()
    udf_sql = udf_sql.replace('\n', ' ')
    pattern = re.compile(r'FUNCTION\s*`?(\w+.)?(\w+)`?\s*\(')
    match = pattern.search(udf_sql)
    if match:
        udf_name = match[2]
        return udf_name
    else:
        return None


def extract_udf_signature(udf_path):
    with open(udf_path) as udf_file:
        udf_sql = udf_file.read()
    udf_sql = udf_sql.replace('\n', ' ')
    pattern = re.compile(r'FUNCTION\s+(`?.+?`?.*?\).*?\s+)AS')
    match = pattern.search(udf_sql)
    if match:
        udf_name = match[1].replace('LANGUAGE js', '')
        return udf_name
    else:
        return udf_path


def replace_with_test_datasets(udf_path=None, project_id=None, udf_sql=None):
    if udf_path:
        with open(udf_path) as udf_file:
            udf_sql = udf_file.read()
    udf_length_before_replacement = len(udf_sql)
    udf_sql = re.sub(
        r'(\w+\.)?(?P<bq_dataset>\w+)(?P<udf_name>\.\w+)\(',
        f'`{project_id}.\\g<bq_dataset>_test_{os.getenv("SHORT_SHA")}\\g<udf_name>`(',
        udf_sql)
    if udf_length_before_replacement == len(udf_sql):
        return None
    else:
        return udf_sql


def get_target_bq_dataset(udf_path):
    parent_dir_name = Path(udf_path).parent.name
    return f'{BIGQUERY_TEST_DATASET_MAPPINGS.get(parent_dir_name)}_{os.getenv("SHORT_SHA")}'


def delete_datasets(client):
    for dataset in BIGQUERY_TEST_DATASET_MAPPINGS.values():
        dataset = f'{dataset}_{os.getenv("SHORT_SHA")}'
        client.delete_dataset(dataset, delete_contents=True, not_found_ok=True)


def create_datasets(client):
    for dataset in BIGQUERY_TEST_DATASET_MAPPINGS.values():
        dataset = f'{dataset}_{os.getenv("SHORT_SHA")}'
        client.create_dataset(dataset, exists_ok=True)


def main():
    parser = argparse.ArgumentParser(description='Utils Class to support testing BigQuery UDFs')
    parser.add_argument('--create_test_datasets', help='Create test datasets used for UDF function testing.',
                        action='store_true')
    parser.add_argument('--delete_test_datasets', help='Delete test datasets used for UDF function testing.',
                        action='store_true')
    args = parser.parse_args()
    client = bigquery.Client()
    if args.create_test_datasets:
        create_datasets(client)
    elif args.delete_test_datasets:
        delete_datasets(client)


if __name__ == '__main__':
    main()
