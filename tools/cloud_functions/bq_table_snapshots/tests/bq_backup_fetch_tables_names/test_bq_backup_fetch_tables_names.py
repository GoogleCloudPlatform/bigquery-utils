# Copyright 2023 Google LLC
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
"""unit tests for bq_backup_fetch_table_names"""

from requests import request
from bq_backup_fetch_tables_names.main import filter_tables, TABLE_TYPE_PHYSICAL_TABLE
import pytest
from google.cloud import bigquery


@pytest.mark.parametrize(
    "tables_to_include,tables_to_exclude,expected",
    [
        ([],[],['table1', 'table2', 'table3']),
        (['table1'],[],['table1']),
        ([],['table1'],['table2', 'table3']),
        (['table1'],['table2'],['table1'])
    ])
def test_filter_tables(tables_to_include, tables_to_exclude, expected):
    """ensure table filters are working properly
    """
    tables = [build_bigquery_table_ref(x) for x in ['table1', 'table2', 'table3']]
    request_json = {
        "tables_to_include_list": tables_to_include,
        "tables_to_exclude_list": tables_to_exclude
    }
    tables = filter_tables(tables, request_json)
    expected = [f"project1.dataset1.{x}" for x in expected]

    assert tables == expected

    
def build_bigquery_table_ref(table_name):
    table_ref = bigquery.Table(f'project1.dataset1.{table_name}')
    table_ref._properties['type'] = TABLE_TYPE_PHYSICAL_TABLE
    return table_ref
