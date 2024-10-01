# Copyright (C) 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import logging
import json
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.api_core import client_info as http_client_info
import base64
import os

TABLE_TYPE_PHYSICAL_TABLE = "TABLE"
# id of project used for BQ storage
DATA_PROJECT_ID = os.environ.get('DATA_PROJECT_ID', 'Specified environment variable is not set.')
# id of project with P/S topic
PUBSUB_PROJECT_ID = os.environ.get('PUBSUB_PROJECT_ID', 'Specified environment variable is not set.')
# name of P/S topic where this code will publish to
TABLE_NAME_PUBSUB_TOPIC_ID = os.environ.get('TABLE_NAME_PUBSUB_TOPIC_ID', 'Specified environment variable is not set.')


def filter_tables(tables, request_json):
    tables_to_include_list = request_json.get("tables_to_include_list", [])
    tables_to_exclude_list = request_json.get("tables_to_exclude_list", [])

    tables = [x for x in tables if x.table_type == TABLE_TYPE_PHYSICAL_TABLE]
    if len(tables_to_include_list) > 0:
        tables = [x for x in tables if x.table_id in tables_to_include_list]
    if len(tables_to_exclude_list) > 0:
        tables = [x for x in tables if x.table_id not in tables_to_exclude_list]
    
    tables = [f"{x.project}.{x.dataset_id}.{x.table_id}" for x in tables]

    return tables 


def get_bq_client():
    client_info = http_client_info.ClientInfo(user_agent=f"google-pso-tool/bq-snapshots/0.0.1")
    client = bigquery.Client(project=DATA_PROJECT_ID, client_info=client_info)
    return client


client = get_bq_client()


def main(event, context):
    """
    request should contain a payload like:
    {
        "source_dataset_name":"DATASET_1",
        "target_dataset_name":"SNAPSHOT_DATASET_1",
        "crontab_format":"10 * * * *",
        "seconds_before_expiration":604800,
        "tables_to_include_list":[],
        "tables_to_exclude_list":[] 
    }
    tables_to_include_list and tables_to_exclude_list are optional
    """
    message = base64.b64decode(event['data']).decode('utf-8')
    request_json = json.loads(message)

    source_dataset = request_json['source_dataset_name']

    publisher = pubsub_v1.PublisherClient()
    table_name_topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, TABLE_NAME_PUBSUB_TOPIC_ID)

    tables = client.list_tables(source_dataset)
    tables = filter_tables(tables, request_json)
    
    for table_name in tables:
        logging.info(f"sending Pub/Sub message for table: {table_name}")
        request_json['table_name'] = table_name
        data = json.dumps(request_json)
        publisher.publish(table_name_topic_path, data.encode("utf-8"))

    return "ok"
