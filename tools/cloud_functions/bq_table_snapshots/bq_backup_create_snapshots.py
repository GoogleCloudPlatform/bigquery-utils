import logging
from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta
from cronsim import CronSim
from google.api_core import client_info as http_client_info
import json 
import logging
import time
import base64
import os

# id of project used for BQ storage
BQ_DATA_PROJECT_ID = os.environ.get('BQ_DATA_PROJECT_ID', 'Specified environment variable is not set.')
# id of project used for BQ compute
BQ_JOBS_PROJECT_ID = os.environ.get('BQ_JOBS_PROJECT_ID', 'Specified environment variable is not set.')


def get_snapshot_timestamp(message):
    cron_format = message['crontab_format']
    it = CronSim(cron_format, datetime.now())
    next_interval = next(it)
    next_next_interval = next(it)
    delta = (next_next_interval - next_interval).total_seconds()
    prev_interval = next_interval - relativedelta(seconds=delta)
    prev_cron_interval_timestamp = int(prev_interval.timestamp() * 1000)
    return prev_cron_interval_timestamp 


def create_snapshot(message):
    target_dataset_name = message['target_dataset_name']
    seconds_before_expiration = message['seconds_before_expiration']

    prev_cron_interval_timestamp = get_snapshot_timestamp(message)

    current_date = datetime.now().strftime("%Y%m%d")
    snapshot_expiration_date = datetime.now() + relativedelta(seconds=int(seconds_before_expiration))

    source_table_fullname = message['table_name']
    source_table_name = source_table_fullname.split(".")[2]
    snapshot_name = f"{BQ_DATA_PROJECT_ID}.{target_dataset_name}.{source_table_name}_{current_date}"
    source_table_fullname = f"{source_table_fullname}@{prev_cron_interval_timestamp}"

    job_config = bigquery.CopyJobConfig()
    job_config.operation_type = "SNAPSHOT"
    job_config._properties["copy"]["destinationExpirationTime"] = snapshot_expiration_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    job = client.copy_table(source_table_fullname, snapshot_name, job_config=job_config)
    logging.info(f"Creating snapshot for table: {snapshot_name}")
    return job

def get_bq_client():
    client_info = http_client_info.ClientInfo(user_agent=f"google-pso-tool/bq-snapshots/0.0.1")
    client = bigquery.Client(project=BQ_JOBS_PROJECT_ID, client_info=client_info)
    return client


client = get_bq_client()


def main(event, context):
    """
    event should containa payload like:
    {
        "source_dataset_name":"DATASET_1",
        "target_dataset_name":"SNAPSHOT_DATASET_1",
        "crontab_format":"0 * * * *",
        "seconds_before_expiration":604800,
        "table_name": "project.dataset.table"
    }
    """
    message = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(message)
    
    job = create_snapshot(message)

    while True:
        if job.done():
            exception = job.exception()
            if exception:
                logging.info(str(exception))
                raise Exception(str(exception))
            else:
                return 'ok'
        time.sleep(2)
