# Copyright 2021 Google LLC.
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
"""Implement function to ensure loading data from GCS to BigQuery in order.
"""
import datetime
import os
import time
import traceback
from typing import Optional, Tuple

import google.api_core
import google.api_core.exceptions
import pytz
# pylint in cloud build is being flaky about this import discovery.
# pylint: disable=no-name-in-module
from google.cloud import bigquery
from google.cloud import storage

from . import constants  # pylint: disable=no-name-in-module,import-error
from . import exceptions  # pylint: disable=no-name-in-module,import-error
from . import utils  # pylint: disable=no-name-in-module,import-error


def backlog_publisher(
    gcs_client: storage.Client,
    event_blob: storage.Blob,
) -> Optional[storage.Blob]:
    """add success files to the the backlog and trigger backfill if necessary"""
    bkt = event_blob.bucket

    # Create an entry in _backlog for this table for this batch / success file
    backlog_blob = success_blob_to_backlog_blob(event_blob)
    backlog_blob.upload_from_string("", client=gcs_client)
    print(f"added gs://{backlog_blob.bucket.name}/{backlog_blob.name} "
          "to the backlog.")

    table_prefix = utils.get_table_prefix(event_blob.name)
    return start_backfill_subscriber_if_not_running(gcs_client, bkt,
                                                    table_prefix)


def backlog_subscriber(gcs_client: Optional[storage.Client],
                       bq_client: Optional[bigquery.Client],
                       backfill_blob: storage.Blob, function_start_time: float):
    """Pick up the table lock, poll BQ job id until completion and process next
    item in the backlog.
    """
    print(f"started backfill subscriber for gs://{backfill_blob.bucket.name}/"
          f"{backfill_blob.name}")
    gcs_client, bq_client = _get_clients_if_none(gcs_client, bq_client)
    # We need to retrigger the backfill loop before the Cloud Functions Timeout.
    restart_time = function_start_time + (
        float(os.getenv("FUNCTION_TIMEOUT_SEC", "60")) -
        constants.RESTART_BUFFER_SECONDS)
    print(f"restart time is {restart_time}")
    bkt = backfill_blob.bucket
    utils.handle_duplicate_notification(gcs_client, backfill_blob)
    table_prefix = utils.get_table_prefix(backfill_blob.name)
    last_job_done = False
    # we will poll for job completion this long in an individual iteration of
    # the while loop (before checking if we are too close to cloud function
    # timeout and should retrigger).
    polling_timeout = 5  # seconds
    lock_blob: storage.Blob = bkt.blob(f"{table_prefix}/_bqlock")
    if restart_time - polling_timeout < time.monotonic():
        raise EnvironmentError(
            "The Cloud Function timeout is too short for "
            "backlog subscriber to do it's job. We recommend "
            "setting the timeout to 540 seconds or at least "
            "1 minute (Cloud Functions default).")
    while time.monotonic() < restart_time - polling_timeout - 1:
        first_bq_lock_claim = False
        lock_contents = utils.read_gcs_file_if_exists(
            gcs_client, f"gs://{bkt.name}/{lock_blob.name}")
        if lock_contents:
            # is this a lock placed by this cloud function.
            # the else will handle a manual _bqlock
            if lock_contents.startswith(
                    os.getenv('JOB_PREFIX', constants.DEFAULT_JOB_PREFIX)):
                last_job_done = wait_on_last_job(bq_client, lock_blob,
                                                 backfill_blob, lock_contents,
                                                 polling_timeout)
            else:
                print(f"sleeping for {polling_timeout} seconds because"
                      f"found manual lock gs://{bkt.name}/{lock_blob.name} with"
                      "This will be an infinite loop until the manual lock is "
                      "released. "
                      f"manual lock contents: {lock_contents}. ")
                time.sleep(polling_timeout)
                continue
        else:  # this condition handles absence of _bqlock file
            first_bq_lock_claim = True
            last_job_done = True  # there's no running job to poll.

        if not last_job_done:
            # keep polling th running job.
            continue

        # if reached here, last job is done.
        if not first_bq_lock_claim:
            # If the BQ lock was missing we do not want to delete a backlog
            # item for a job we have not yet submitted.
            utils.remove_oldest_backlog_item(gcs_client, bkt, table_prefix)
        should_subscriber_exit = handle_backlog(gcs_client, bq_client, bkt,
                                                lock_blob, backfill_blob)
        if should_subscriber_exit:
            return
    # retrigger the subscriber loop by reposting the _BACKFILL file
    print("ran out of time, restarting backfill subscriber loop for:"
          f"gs://{bkt.name}/{table_prefix}")
    backfill_blob = bkt.blob(f"{table_prefix}/{constants.BACKFILL_FILENAME}")
    backfill_blob.upload_from_string("")


def wait_on_last_job(bq_client: bigquery.Client, lock_blob: storage.Blob,
                     backfill_blob: storage.blob, job_id: str,
                     polling_timeout: int):
    """wait on a bigquery job or raise informative exception.

    Args:
        bq_client: bigquery.Client
        lock_blob: storage.Blob _bqlock blob
        backfill_blob: storage.blob _BACKFILL blob
        job_id: str BigQuery job ID to wait on (read from _bqlock file)
        polling_timeout: int seconds to poll before returning.
    """
    try:
        return utils.wait_on_bq_job_id(bq_client, job_id, polling_timeout)
    except (exceptions.BigQueryJobFailure,
            google.api_core.exceptions.NotFound) as err:
        table_prefix = utils.get_table_prefix(backfill_blob.name)
        raise exceptions.BigQueryJobFailure(
            f"previous BigQuery job: {job_id} failed or could not "
            "be found. This will kill the backfill subscriber for "
            f"the table prefix: {table_prefix}."
            "Once the issue is dealt with by a human, the lock "
            "file at: "
            f"gs://{lock_blob.bucket.name}/{lock_blob.name} "
            "should be manually removed and a new empty "
            f"{constants.BACKFILL_FILENAME} "
            "file uploaded to: "
            f"gs://{backfill_blob.bucket.name}/{table_prefix}"
            "/_BACKFILL "
            f"to resume the backfill subscriber so it can "
            "continue with the next item in the backlog."
            "Original Exception:"
            f"{traceback.format_exc()}") from err


def handle_backlog(
    gcs_client: storage.Client,
    bq_client: bigquery.Client,
    bkt: storage.Bucket,
    lock_blob: storage.Blob,
    backfill_blob: storage.Blob,
):
    """submit the next item in the _backlog if it is non-empty or clean up the
    _BACKFILL and _bqlock files.
    Args:
        gcs_client: storage.Client
        bq_client: bigquery.Client
        bkt: storage.Bucket
        lock_blob: storage.Blob _bqlock blob
        backfill_blob: storage.blob _BACKFILL blob
    Returns:
        bool: should this backlog subscriber exit
    """
    table_prefix = utils.get_table_prefix(backfill_blob.name)
    check_backlog_time = time.monotonic()
    next_backlog_file = utils.get_next_backlog_item(gcs_client, bkt,
                                                    table_prefix)
    if next_backlog_file:
        next_success_file: storage.Blob = bkt.blob(
            next_backlog_file.name.replace("/_backlog/", "/"))
        if not next_success_file.exists(client=gcs_client):
            raise exceptions.BacklogException(
                "backlog contains "
                f"gs://{next_backlog_file.bucket}/{next_backlog_file.name} "
                "but the corresponding success file does not exist at: "
                f"gs://{next_success_file.bucket}/{next_success_file.name}")
        print("applying next batch for:"
              f"gs://{next_success_file.bucket}/{next_success_file.name}")
        next_job_id = utils.create_job_id(next_success_file.name)
        utils.apply(gcs_client, bq_client, next_success_file, lock_blob,
                    next_job_id)
        return False  # BQ job running
    print("no more files found in the backlog deleteing backfill blob")
    backfill_blob.delete(if_generation_match=backfill_blob.generation,
                         client=gcs_client)
    if (check_backlog_time + constants.ENSURE_SUBSCRIBER_SECONDS <
            time.monotonic()):
        print("checking if the backlog is still empty for "
              f"gs://${bkt.name}/{table_prefix}/_backlog/"
              f"There was more than {constants.ENSURE_SUBSCRIBER_SECONDS}"
              " seconds between listing items on the backlog and "
              f"deleting the {constants.BACKFILL_FILENAME}. "
              "This should not happen often but is meant to alleviate a "
              "race condition in the event that something caused the "
              "delete operation was delayed or had to be retried for a "
              "long time.")
        next_backlog_file = utils.get_next_backlog_item(gcs_client, bkt,
                                                        table_prefix)
        if next_backlog_file:
            # The backfill file was deleted but the backlog is
            # not empty. Re-trigger the backfill subscriber loop by
            # dropping a new backfill file.
            start_backfill_subscriber_if_not_running(gcs_client, bkt,
                                                     table_prefix)
            return True  # we are re-triggering a new backlog subscriber
    utils.handle_bq_lock(gcs_client, lock_blob, None)
    print(f"backlog is empty for gs://{bkt.name}/{table_prefix}. "
          "backlog subscriber exiting.")
    return True  # the backlog is empty


def start_backfill_subscriber_if_not_running(
        gcs_client: Optional[storage.Client], bkt: storage.Bucket,
        table_prefix: str) -> Optional[storage.Blob]:
    """start the backfill subscriber if  it is not already runnning for this
    table prefix.

    created a backfill file for the table prefix if not exists.
    """
    if not gcs_client:
        gcs_client = storage.Client(client_info=constants.CLIENT_INFO)
    start_backfill = True
    # Do not start subscriber until START_BACKFILL_FILENAME has been dropped
    # at the table prefix.
    if constants.START_BACKFILL_FILENAME:
        start_backfill_blob = bkt.blob(
            f"{table_prefix}/{constants.START_BACKFILL_FILENAME}")
        start_backfill = start_backfill_blob.exists(client=gcs_client)
        if not start_backfill:
            print("note triggering backfill because"
                  f"gs://{start_backfill_blob.bucket.name}/"
                  f"{start_backfill_blob.name} was not found.")

    if start_backfill:
        # Create a _BACKFILL file for this table if not exists
        backfill_blob = bkt.blob(
            f"{table_prefix}/{constants.BACKFILL_FILENAME}")
        try:
            backfill_blob.upload_from_string("",
                                             if_generation_match=0,
                                             client=gcs_client)
            print("triggered backfill with "
                  f"gs://{backfill_blob.bucket.name}/{backfill_blob.name} "
                  f"created at {backfill_blob.time_created}.")
            return backfill_blob
        except google.api_core.exceptions.PreconditionFailed:
            backfill_blob.reload(client=gcs_client)
            print("backfill already in progress due to: "
                  f"gs://{backfill_blob.bucket.name}/{backfill_blob.name} "
                  f"created at {backfill_blob.time_created}. exiting.")
            return backfill_blob
    else:
        return None


def success_blob_to_backlog_blob(success_blob: storage.Blob) -> storage.Blob:
    """create a blob object that is a pointer to the input success blob in the
    backlog
    """
    bkt = success_blob.bucket
    table_prefix = utils.get_table_prefix(success_blob.name)
    success_file_suffix = utils.removeprefix(success_blob.name,
                                             f"{table_prefix}/")
    return bkt.blob(f"{table_prefix}/_backlog/{success_file_suffix}")


def subscriber_monitor(gcs_client: Optional[storage.Client],
                       bkt: storage.Bucket, object_id: str) -> bool:
    """
    Monitor to handle a rare race condition where:

    1. subscriber reads an empty backlog (before it can delete the
      _BACKFILL blob...)
    2. a new item is added to the backlog (causing a separate
       function invocation)
    3. In this new invocation we reach this point in the code path
       and start_backlog_subscriber_if_not_running sees the old _BACKFILL
       and does not create a new one.
    4. The subscriber deletes the _BACKFILL blob and exits without
       processing the new item on the backlog from #2.

    We handle this by success file added to the backlog starts this monitoring
    to wait constants.ENSURE_SUBSCRIBER_SECONDS before checking that the
    backfill file exists. On the subscriber side we check if there was more time
    than this between list backlog items and delete backfill calls. This way
    we always handle this race condition either in this monitor or in the
    subscriber itself.
    """
    if not gcs_client:
        gcs_client = storage.Client(client_info=constants.CLIENT_INFO)
    backfill_blob = start_backfill_subscriber_if_not_running(
        gcs_client, bkt, utils.get_table_prefix(object_id))

    # backfill blob may be none if the START_BACKFILL_FILENAME has not been
    # dropped
    if backfill_blob:
        # Handle case where a subscriber loop was not able to repost the
        # backfill file before the cloud function timeout.
        time_created_utc = backfill_blob.time_created.replace(tzinfo=pytz.UTC)
        now_utc = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
        if (now_utc - time_created_utc > datetime.timedelta(
                seconds=int(os.getenv("FUNCTION_TIMEOUT_SEC", "60")))):
            print(
                f"backfill blob gs://{backfill_blob.bucket.name}/"
                f"{backfill_blob.name} appears to be abandoned as it is older "
                "than the cloud function timeout of "
                f"{os.getenv('FUNCTION_TIMEOUT_SEC', '60')} seconds."
                "reposting this backfill blob to restart the backfill"
                "subscriber for this table.")
            backfill_blob.delete(client=gcs_client)
            start_backfill_subscriber_if_not_running(
                gcs_client, bkt, utils.get_table_prefix(object_id))
            return True

        time.sleep(constants.ENSURE_SUBSCRIBER_SECONDS)
        while not utils.wait_on_gcs_blob(gcs_client, backfill_blob,
                                         constants.ENSURE_SUBSCRIBER_SECONDS):
            start_backfill_subscriber_if_not_running(
                gcs_client, bkt, utils.get_table_prefix(object_id))
            return True
    return False


def _get_clients_if_none(
    gcs_client: Optional[storage.Client], bq_client: Optional[bigquery.Client]
) -> Tuple[storage.Client, bigquery.Client]:
    """method to handle case where clients are None.

    This is a workaround to be able to run the backlog subscriber in a separate
    process to facilitate some of our integration tests. Though it should be
    harmless if these clients are recreated in the Cloud Function.
    """
    print("instantiating missing clients in backlog subscriber this should only"
          " happen during integration tests.")
    if not gcs_client:
        gcs_client = storage.Client(client_info=constants.CLIENT_INFO)
    if not bq_client:
        default_query_config = bigquery.QueryJobConfig()
        default_query_config.use_legacy_sql = False
        default_query_config.labels = constants.DEFAULT_JOB_LABELS
        bq_client = bigquery.Client(
            client_info=constants.CLIENT_INFO,
            default_query_job_config=default_query_config,
            project=os.getenv("BQ_PROJECT", os.getenv("GCP_PROJECT")))
    return gcs_client, bq_client
