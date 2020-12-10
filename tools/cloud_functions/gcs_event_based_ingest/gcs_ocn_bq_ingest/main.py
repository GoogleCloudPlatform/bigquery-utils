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
import time
from typing import Dict

# pylint in cloud build is being flaky about this import discovery.
# pylint: disable=no-name-in-module
from google.cloud import bigquery
from google.cloud import error_reporting
from google.cloud import storage

from . import constants
from . import exceptions
from . import ordering
from . import utils

# Reuse GCP Clients across function invocations using globbals
# https://cloud.google.com/functions/docs/bestpractices/tips#use_global_variables_to_reuse_objects_in_future_invocations
# pylint: disable=global-statement

ERROR_REPORTING_CLIENT = None

BQ_CLIENT = None

GCS_CLIENT = None


def main(event: Dict, context):  # pylint: disable=unused-argument
    """entry point for background cloud function for event driven GCS to
    BigQuery ingest."""
    try:
        function_start_time = time.monotonic()
        # pylint: disable=too-many-locals

        bucket_id, object_id = utils.parse_notification(event)

        basename_object_id = os.path.basename(object_id)

        # Exit eagerly if this is not a file to take action on
        # (e.g. a data, config, or lock file)
        if basename_object_id not in constants.ACTION_FILENAMES:
            action_filenames = constants.ACTION_FILENAMES
            if constants.START_BACKFILL_FILENAME is None:
                action_filenames.remove(None)
            print(f"No-op. This notification was not for a"
                  f"{action_filenames} file.")
            return

        gcs_client = lazy_gcs_client()
        bq_client = lazy_bq_client()
        table_ref, batch = utils.gcs_path_to_table_ref_and_batch(object_id)

        enforce_ordering = (constants.ORDER_PER_TABLE
                            or utils.look_for_config_in_parents(
                                gcs_client, f"gs://{bucket_id}/{object_id}",
                                "ORDERME") is not None)

        bkt: storage.Bucket = utils.cached_get_bucket(gcs_client, bucket_id)
        event_blob: storage.Blob = bkt.blob(object_id)

        if enforce_ordering:
            # For SUCCESS files in a backlog directory, ensure that subscriber
            # is running.
            if (basename_object_id == constants.SUCCESS_FILENAME
                    and "/_backlog/" in object_id):
                print(
                    f"This notification was for "
                    f"gs://{bucket_id}/{object_id} a"
                    f"{constants.SUCCESS_FILENAME} in a"
                    "/_backlog/ directory. "
                    f"Watiting {constants.ENSURE_SUBSCRIBER_SECONDS} seconds to "
                    "ensure that subscriber is running.")
                ordering.subscriber_monitor(gcs_client, bkt, object_id)
                return
            if (constants.START_BACKFILL_FILENAME and basename_object_id
                    == constants.START_BACKFILL_FILENAME):
                # This will be the first backfill file.
                ordering.start_backfill_subscriber_if_not_running(
                    gcs_client, bkt, utils.get_table_prefix(object_id))
                return
            if basename_object_id == constants.SUCCESS_FILENAME:
                ordering.backlog_publisher(gcs_client, event_blob)
                return
            if basename_object_id == constants.BACKFILL_FILENAME:
                ordering.backlog_subscriber(gcs_client, bq_client, event_blob,
                                            function_start_time)
                return
        else:  # Default behavior submit job as soon as success file lands.
            if basename_object_id == constants.SUCCESS_FILENAME:
                utils.handle_duplicate_notification(event_blob)
                utils.apply(
                    gcs_client,
                    bq_client,
                    event_blob,
                    # None lock blob as there is no serialization required.
                    None,
                    utils.create_job_id(table_ref, batch))
    # Unexpected exceptions will actually raise which may cause a cold restart.
    except tuple(exceptions.EXCEPTIONS_TO_REPORT) as original_error:
        # We do this because we know these errors do not require a cold restart
        # of the cloud function.
        try:
            lazy_error_reporting_client().report_exception()
        except Exception:  # pylint: disable=broad-except
            # This mostly handles the case where error reporting API is not
            # enabled or IAM permissions did not allow us to report errors with
            # error reporting API.
            raise original_error  # pylint: disable=raise-missing-from


def lazy_error_reporting_client() -> error_reporting.Client:
    """
    Return a error reporting client that may be shared between cloud function
    invocations.

    https://cloud.google.com/functions/docs/monitoring/error-reporting
    """
    global ERROR_REPORTING_CLIENT
    if not ERROR_REPORTING_CLIENT:
        ERROR_REPORTING_CLIENT = error_reporting.Client(
            client_info=constants.CLIENT_INFO)
    return ERROR_REPORTING_CLIENT


def lazy_bq_client() -> bigquery.Client:
    """
    Return a BigQuery Client that may be shared between cloud function
    invocations.
    """
    global BQ_CLIENT
    if not BQ_CLIENT:
        default_query_config = bigquery.QueryJobConfig()
        default_query_config.use_legacy_sql = False
        default_query_config.labels = constants.DEFAULT_JOB_LABELS
        BQ_CLIENT = bigquery.Client(
            client_info=constants.CLIENT_INFO,
            default_query_job_config=default_query_config,
            project=os.getenv("BQ_PROJECT", os.getenv("GCP_PROJECT")))
    return BQ_CLIENT


def lazy_gcs_client() -> storage.Client:
    """
    Return a BigQuery Client that may be shared between cloud function
    invocations.
    """
    global GCS_CLIENT
    if not GCS_CLIENT:
        GCS_CLIENT = storage.Client(client_info=constants.CLIENT_INFO)
    return GCS_CLIENT
