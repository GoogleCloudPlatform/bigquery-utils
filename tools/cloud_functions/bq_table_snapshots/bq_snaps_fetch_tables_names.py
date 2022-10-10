import logging
import json
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.api_core import client_info as http_client_info
import datetime

TABLE_TYPE_PHYSICAL_TABLE = ""
# id of project used for BQ storage
DATA_PROJECT_ID = ""
# id of project with P/S topic
PUBSUB_PROJECT_ID = ""
# name of P/S topic where this code will publish to
TABLE_NAME_PUBSUB_TOPIC_ID = "bq-snap-table-names"
# filters
DATASETS_TO_INCLUDE = []
DATASETS_TO_EXCLUDE = []
TABLES_TO_INCLUDE = []
TABLES_TO_EXCLUDE = []
# snapshot duration in seconds
SECONDS_BEFORE_EXPIRATION = 2592000


def filter_datasets(datasets):
    if len(DATASETS_TO_INCLUDE) > 0:
        datasets = [x for x in datasets if x in DATASETS_TO_INCLUDE]
    if len(DATASETS_TO_EXCLUDE) > 0:
        datasets = [x for x in datasets if x not in DATASETS_TO_EXCLUDE]

    return datasets


def filter_tables(tables):
    if len(TABLES_TO_INCLUDE) > 0:
        tables = [x for x in tables if x in TABLES_TO_INCLUDE]
    if len(TABLES_TO_EXCLUDE) > 0:
        tables = [x for x in tables if x not in TABLES_TO_EXCLUDE]
    return tables 
    

def get_bq_client():
    client_info = http_client_info.ClientInfo(user_agent=f"google-pso-tool/bq-snapshots/0.0.1")
    client = bigquery.Client(project=DATA_PROJECT_ID, client_info=client_info)
    return client


def get_tables(dataset_name, client):
    logging.info(f"getting tables for dataset: {DATA_PROJECT_ID}.{dataset_name}")
    tables = client.list_tables(dataset_name)
    tables = [x for x in tables if x.table_type == TABLE_TYPE_PHYSICAL_TABLE]
    tables = [x.table_id for x in tables]
    tables = filter_tables(tables)
    return tables


def get_datasets(client):
    logging.info(f"getting datasets for project: {DATA_PROJECT_ID}")
    datasets = client.list_datasets(DATA_PROJECT_ID)
    datasets = [x.dataset_id for x in datasets]
    datasets = filter_datasets(datasets)
    return datasets    


def publish_to_pubsub(dataset_name, table_name, snapshot_timestamp, publisher, table_name_topic_path):
    logging.info(f"sending Pub/Sub message for table: {DATA_PROJECT_ID}.{dataset_name}.{table_name}")
    request_json = {
        'source_project_id': DATA_PROJECT_ID,
        'source_dataset_name': dataset_name,
        'source_table_name': table_name,
        'snapshot_timestamp': snapshot_timestamp,
        'seconds_before_expiration': SECONDS_BEFORE_EXPIRATION
    }
    data = json.dumps(request_json)
    publisher.publish(table_name_topic_path, data.encode("utf-8"))

    
def hello_pubsub(event, context):
    snapshot_timestamp = int(datetime.datetime.now().timestamp()*1000)
    publisher = pubsub_v1.PublisherClient()
    table_name_topic_path = publisher.topic_path(PUBSUB_PROJECT_ID, TABLE_NAME_PUBSUB_TOPIC_ID)

    client = get_bq_client()
    datasets = get_datasets(client)
    for dataset_name in datasets:
        tables = get_tables(dataset_name, client)
        for table_name in tables:
            publish_to_pubsub(dataset_name, table_name, 
                            snapshot_timestamp, publisher, 
                            table_name_topic_path)
    return "ok"
