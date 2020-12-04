# Copyright 2020 Google LLC.
# This software is provided as-is, without warranty or representation
# for any use or purpose.
# Your use of it is subject to your agreement with Google.

# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Background Cloud Function for loading data from GCS to BigQuery.
"""
import os
import re
from typing import Dict

import google.api_core.client_info
import google.cloud.exceptions
from google.cloud import bigquery, storage

from .utils import (DEFAULT_JOB_LABELS, SUCCESS_FILENAME, cached_get_bucket,
                    create_job_id_prefix, external_query,
                    handle_duplicate_notification, load_batches,
                    look_for_config_in_parents, parse_notification,
                    read_gcs_file_if_exists, removesuffix)

# yapf: disable
DEFAULT_DESTINATION_REGEX = (
    r"^(?P<dataset>[\w\-\._0-9]+)/"  # dataset (required)
    r"(?P<table>[\w\-_0-9]+)/?"      # table name (required)
    r"(?P<partition>\$[0-9]+)?/?"    # partition decorator (optional)
    r"(?P<yyyy>[0-9]{4})?/?"         # partition year (yyyy) (optional)
    r"(?P<mm>[0-9]{2})?/?"           # partition month (mm) (optional)
    r"(?P<dd>[0-9]{2})?/?"           # partition day (dd)  (optional)
    r"(?P<hh>[0-9]{2})?/?"           # partition hour (hh) (optional)
    r"(?P<batch>[\w\-_0-9]+)?/"      # batch id (optional)
)
# yapf: enable

CLIENT_INFO = google.api_core.client_info.ClientInfo(
    user_agent="google-pso-tool/bq-severless-loader")


def main(event: Dict, context):  # pylint: disable=unused-argument
    """entry point for background cloud function for event driven GCS to
    BigQuery ingest."""
    # pylint: disable=too-many-locals
    # Set by Cloud Function Execution Environment
    # https://cloud.google.com/functions/docs/env-var
    destination_regex = os.getenv("DESTINATION_REGEX",
                                  DEFAULT_DESTINATION_REGEX)
    dest_re = re.compile(destination_regex)

    bucket_id, object_id = parse_notification(event)

    # Exit eagerly if not a success file.
    # we can improve this with pub/sub message filtering once it supports
    # a hasSuffix filter function (we can filter on hasSuffix successfile name)
    #  https://cloud.google.com/pubsub/docs/filtering
    if not object_id.endswith(f"/{SUCCESS_FILENAME}"):
        print(
            f"No-op. This notification was not for a {SUCCESS_FILENAME} file.")
        return

    prefix_to_load = removesuffix(object_id, SUCCESS_FILENAME)
    gsurl = f"gs://{bucket_id}/{prefix_to_load}"
    gcs_client = storage.Client(client_info=CLIENT_INFO)
    project = os.getenv("BQ_PROJECT", gcs_client.project)
    bkt = cached_get_bucket(gcs_client, bucket_id)
    success_blob: storage.Blob = bkt.blob(object_id)
    handle_duplicate_notification(bkt, success_blob, gsurl)

    destination_match = dest_re.match(object_id)
    if not destination_match:
        raise RuntimeError(f"Object ID {object_id} did not match regex:"
                           f" {destination_regex}")
    destination_details = destination_match.groupdict()
    try:
        dataset = destination_details['dataset']
        table = destination_details['table']
    except KeyError:
        raise RuntimeError(
            f"Object ID {object_id} did not match dataset and table in regex:"
            f" {destination_regex}") from KeyError
    partition = destination_details.get('partition')
    year, month, day, hour = (
        destination_details.get(key, "") for key in ('yyyy', 'mm', 'dd', 'hh'))
    part_list = (year, month, day, hour)
    if not partition and any(part_list):
        partition = '$' + ''.join(part_list)
    batch_id = destination_details.get('batch')
    labels = DEFAULT_JOB_LABELS
    labels["bucket"] = bucket_id

    if batch_id:
        labels["batch-id"] = batch_id

    if partition:
        dest_table_ref = bigquery.TableReference.from_string(
            f"{dataset}.{table}{partition}", default_project=project)
    else:
        dest_table_ref = bigquery.TableReference.from_string(
            f"{dataset}.{table}", default_project=project)

    default_query_config = bigquery.QueryJobConfig()
    default_query_config.use_legacy_sql = False
    default_query_config.labels = labels
    bq_client = bigquery.Client(client_info=CLIENT_INFO,
                                default_query_job_config=default_query_config)

    print("looking for bq_transform.sql")
    external_query_sql = read_gcs_file_if_exists(
        gcs_client, f"{gsurl}_config/bq_transform.sql")
    if not external_query_sql:
        external_query_sql = look_for_config_in_parents(gcs_client, gsurl,
                                                        "bq_transform.sql")
    if external_query_sql:
        print("EXTERNAL QUERY")
        print(f"found external query:\n{external_query_sql}")
        external_query(gcs_client, bq_client, gsurl, external_query_sql,
                       dest_table_ref,
                       create_job_id_prefix(dest_table_ref, batch_id))
        return

    print("LOAD_JOB")
    load_batches(gcs_client, bq_client, gsurl, dest_table_ref,
                 create_job_id_prefix(dest_table_ref, batch_id))
