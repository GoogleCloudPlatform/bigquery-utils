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
"""Contains utility methods used by the BQIngest process
"""
import collections
import collections.abc
import copy
import json
import os
import pathlib
import time
from typing import Any, Deque, Dict, List, Optional, Tuple

import cachetools
import google.api_core.client_info
import google.api_core.exceptions
import google.cloud.exceptions
from google.cloud import bigquery, storage

# pylint in cloud build is being flaky about this import discovery.
from . import constants  # pylint: disable=no-name-in-module


def create_job_id_prefix(dest_table_ref: bigquery.TableReference,
                         batch_id: Optional[str]):
    """Create job id prefix with a consistent naming convention.
    The naming conventions is as follows:
    gcf-ingest-<dataset_id>-<table_id>-<partition_num>-<batch_id>-
    Parts that are not inferrable from the GCS path with have a 'None'
    placeholder. This naming convention is crucial for monitoring the system.
    Note, gcf-ingest- can be overridden with environment variable JOB_PREFIX

    Examples:

    Non-partitioned Non batched tables:
      - gs://${BUCKET}/tpch/lineitem/_SUCCESS
      - gcf-ingest-tpch-lineitem-None-None-
    Non-partitioned batched tables:
      - gs://${BUCKET}/tpch/lineitem/batch000/_SUCCESS
      - gcf-ingest-tpch-lineitem-None-batch000-
    Partitioned Batched tables:
      - gs://${BUCKET}/tpch/lineitem/$20201031/batch000/_SUCCESS
      - gcf-ingest-tpch-lineitem-20201031-batch000-
    """
    table_partition = dest_table_ref.table_id.split("$")
    if len(table_partition) < 2:
        # If there is no partition put a None placeholder
        table_partition.append("None")
    return f"{os.getenv('JOB_PREFIX', constants.DEFAULT_JOB_PREFIX)}" \
        f"{dest_table_ref.dataset_id}-" \
        f"{'-'.join(table_partition)}-" \
        f"{batch_id}-"


def external_query(  # pylint: disable=too-many-arguments
        gcs_client: storage.Client, bq_client: bigquery.Client, gsurl: str,
        query: str, dest_table_ref: bigquery.TableReference,
        job_id_prefix: str):
    """Load from query over external table from GCS.

    This hinges on a SQL query defined in GCS at _config/bq_transform.sql and
    an external table definition _config/external.json (otherwise will assume
    CSV external table)
    """
    external_table_config = read_gcs_file_if_exists(
        gcs_client, f"{gsurl}_config/external.json")
    if not external_table_config:
        external_table_config = look_for_config_in_parents(
            gcs_client, gsurl, "external.json")
    if external_table_config:
        external_table_def = json.loads(external_table_config)
    else:
        print(f"Falling back to default CSV external table."
              f" {gsurl}_config/external.json not found.")
        external_table_def = constants.DEFAULT_EXTERNAL_TABLE_DEFINITION

    external_table_def["sourceUris"] = flatten2dlist(
        get_batches_for_prefix(gcs_client, gsurl))
    print(f"external table def = {json.dumps(external_table_config, indent=2)}")
    external_config = bigquery.ExternalConfig.from_api_repr(external_table_def)
    job_config = bigquery.QueryJobConfig(
        table_definitions={"temp_ext": external_config}, use_legacy_sql=False)

    # Note, dest_table might include a partition decorator.
    rendered_query = query.format(
        dest_dataset=dest_table_ref.dataset_id,
        dest_table=dest_table_ref.table_id,
    )

    job: bigquery.QueryJob = bq_client.query(
        rendered_query,
        job_config=job_config,
        job_id_prefix=job_id_prefix,
    )

    print(f"started asynchronous query job: {job.job_id}")

    start_poll_for_errors = time.monotonic()
    # Check if job failed quickly
    while time.monotonic(
    ) - start_poll_for_errors < constants.WAIT_FOR_JOB_SECONDS:
        job.reload()
        if job.errors:
            raise RuntimeError(
                f"query job {job.job_id} failed quickly: {job.errors}")
        time.sleep(constants.JOB_POLL_INTERVAL_SECONDS)


def flatten2dlist(arr: List[List[Any]]) -> List[Any]:
    """Flatten list of lists to flat list of elements"""
    return [j for i in arr for j in i]


def load_batches(gcs_client, bq_client, gsurl, dest_table_ref, job_id_prefix):
    """orchestrate 1 or more load jobs based on number of URIs and total byte
    size of objects at gsurl"""
    batches = get_batches_for_prefix(gcs_client, gsurl)
    load_config = construct_load_job_config(gcs_client, gsurl)
    load_config.labels = constants.DEFAULT_JOB_LABELS
    batch_count = len(batches)

    jobs: List[bigquery.LoadJob] = []
    for batch_num, batch in enumerate(batches):
        print(load_config.to_api_repr())
        job: bigquery.LoadJob = bq_client.load_table_from_uri(
            batch,
            dest_table_ref,
            job_config=load_config,
            job_id_prefix=f"{job_id_prefix}{batch_num}-of-{batch_count}-",
        )

        print(f"started asyncronous bigquery load job with id: {job.job_id} for"
              f" {gsurl}")
        jobs.append(job)

    start_poll_for_errors = time.monotonic()
    # Check if job failed quickly
    while time.monotonic(
    ) - start_poll_for_errors < constants.WAIT_FOR_JOB_SECONDS:
        # Check if job failed quickly
        for job in jobs:
            job.reload()
            if job.errors:
                raise RuntimeError(
                    f"load job {job.job_id} failed quickly: {job.errors}")
        time.sleep(constants.JOB_POLL_INTERVAL_SECONDS)


def handle_duplicate_notification(bkt: storage.Bucket,
                                  success_blob: storage.Blob, gsurl: str):
    """
    Need to handle potential duplicate Pub/Sub notifications.
    To achieve this we will drop an empty "claimed" file that indicates
    an invocation of this cloud function has picked up the success file
    with a certain creation timestamp. This will support republishing the
    success file as a mechanism of re-running the ingestion while avoiding
    duplicate ingestion due to multiple Pub/Sub messages for a success file
    with the same creation time.
    """
    success_blob.reload()
    success_created_unix_timestamp = success_blob.time_created.timestamp()

    claim_blob: storage.Blob = bkt.blob(
        success_blob.name.replace(constants.SUCCESS_FILENAME,
                                  f"_claimed_{success_created_unix_timestamp}"))
    try:
        claim_blob.upload_from_string("", if_generation_match=0)
    except google.api_core.exceptions.PreconditionFailed as err:
        raise RuntimeError(
            f"The prefix {gsurl} appears to already have been claimed for "
            f"{gsurl}{constants.SUCCESS_FILENAME} with created timestamp"
            f"{success_created_unix_timestamp}."
            "This means that another invocation of this cloud function has"
            "claimed the ingestion of this batch."
            "This may be due to a rare duplicate delivery of the Pub/Sub "
            "storage notification.") from err


def _get_parent_config_file(storage_client, config_filename, bucket, path):
    config_dir_name = "_config"
    parent_path = pathlib.Path(path).parent
    config_path = parent_path / config_dir_name / config_filename
    return read_gcs_file_if_exists(storage_client,
                                   f"gs://{bucket}/{config_path}")


def look_for_config_in_parents(storage_client: storage.Client, gsurl: str,
                               config_filename: str) -> Optional[str]:
    """look in parent directories for _config/config_filename"""
    blob: storage.Blob = storage.Blob.from_string(gsurl)
    bucket_name = blob.bucket.name
    obj_path = blob.name
    parts = removesuffix(obj_path, "/").split("/")

    def _get_parent_config(path):
        return _get_parent_config_file(storage_client, config_filename,
                                       bucket_name, path)

    config = None
    while parts:
        if config:
            return config
        config = _get_parent_config("/".join(parts))
        parts.pop()
    return config


def construct_load_job_config(storage_client: storage.Client,
                              gsurl: str) -> bigquery.LoadJobConfig:
    """
    merge dictionaries for loadjob.json configs in parent directories.
    The configs closest to gsurl should take precedence.
    """
    config_filename = "load.json"
    blob: storage.Blob = storage.Blob.from_string(gsurl)
    bucket_name = blob.bucket.name
    obj_path = blob.name
    parts = removesuffix(obj_path, "/").split("/")

    def _get_parent_config(path):
        return _get_parent_config_file(storage_client, config_filename,
                                       bucket_name, path)

    config_q: Deque[Dict[str, Any]] = collections.deque()
    config_q.append(constants.BASE_LOAD_JOB_CONFIG)
    while parts:
        config = _get_parent_config("/".join(parts))
        if config:
            config_q.append(json.loads(config))
        parts.pop()

    merged_config: Dict = {}
    while config_q:
        recursive_update(merged_config, config_q.popleft(), in_place=True)
    print(f"merged_config: {merged_config}")
    return bigquery.LoadJobConfig.from_api_repr({"load": merged_config})


def get_batches_for_prefix(
        gcs_client: storage.Client,
        prefix_path: str,
        ignore_subprefix="_config/",
        ignore_file=constants.SUCCESS_FILENAME) -> List[List[str]]:
    """
    This function creates batches of GCS uris for a given prefix.
    This prefix could be a table prefix or a partition prefix inside a
    table prefix.
    returns an Array of their batches
    (one batch has an array of multiple GCS uris)
    """
    batches = []
    blob: storage.Blob = storage.Blob.from_string(prefix_path)
    bucket_name = blob.bucket.name
    prefix_name = blob.name

    prefix_filter = f"{prefix_name}"
    bucket = cached_get_bucket(gcs_client, bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix_filter, delimiter="/"))

    cumulative_bytes = 0
    max_batch_size = int(
        os.getenv("MAX_BATCH_BYTES", constants.DEFAULT_MAX_BATCH_BYTES))
    batch: List[str] = []
    for blob in blobs:
        # API returns root prefix also. Which should be ignored.
        # Similarly, the _SUCCESS file should be ignored.
        # Finally, anything in the _config/ prefix should be ignored.
        if (blob.name
                not in {f"{prefix_name}/", f"{prefix_name}/{ignore_file}"}
                or blob.name.startswith(f"{prefix_name}/{ignore_subprefix}")):
            if blob.size == 0:  # ignore empty files
                print(f"ignoring empty file: gs://{bucket}/{blob.name}")
                continue
            cumulative_bytes += blob.size

            # keep adding until we reach threshold
            if cumulative_bytes <= max_batch_size or len(
                    batch) > constants.MAX_SOURCE_URIS_PER_LOAD:
                batch.append(f"gs://{bucket_name}/{blob.name}")
            else:
                batches.append(batch.copy())
                batch.clear()
                batch.append(f"gs://{bucket_name}/{blob.name}")
                cumulative_bytes = blob.size

    # pick up remaining files in the final batch
    if len(batch) > 0:
        batches.append(batch.copy())
        batch.clear()

    if len(batches) > 1:
        print(f"split into {len(batches)} load jobs.")
    elif len(batches) == 1:
        print("using single load job.")
    else:
        raise RuntimeError("No files to load!")
    return batches


def parse_notification(notification: dict) -> Tuple[str, str]:
    """valdiates notification payload
    Args:
        notification(dict): Pub/Sub Storage Notification
        https://cloud.google.com/storage/docs/pubsub-notifications
        Or Cloud Functions direct trigger
        https://cloud.google.com/functions/docs/tutorials/storage
        with notification schema
        https://cloud.google.com/storage/docs/json_api/v1/objects#resource
    Returns:
        tuple of bucketId and objectId attributes
    Raises:
        KeyError if the input notification does not contain the expected
        attributes.
    """
    if notification.get("kind") == "storage#object":
        # notification is GCS Object reosource from Cloud Functions trigger
        # https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        return notification["bucket"], notification["name"]
    if notification.get("attributes"):
        # notification is Pub/Sub message.
        try:
            attributes = notification["attributes"]
            return attributes["bucketId"], attributes["objectId"]
        except KeyError:
            raise RuntimeError(
                "Issue with Pub/Sub message, did not contain expected"
                f"attributes: 'bucketId' and 'objectId': {notification}"
            ) from KeyError
    raise RuntimeError(
        "Cloud Function recieved unexpected trigger:\n"
        f"{notification}\n"
        "This function only supports direct Cloud Functions"
        "Background Triggers or Pub/Sub storage notificaitons"
        "as described in the following links:\n"
        "https://cloud.google.com/storage/docs/pubsub-notifications\n"
        "https://cloud.google.com/functions/docs/tutorials/storage")


# cache lookups against GCS API for 1 second as buckets / objects have update
# limit of once per second and we might do several of the same lookup during
# the functions lifetime. This should improve performance by eliminating
# unnecessary API calls. The lookups on bucket and objects in this function
# should not be changing during the function's lifetime as this would lead to
# non-deterministic results with or without this cache.
# https://cloud.google.com/storage/quotas
@cachetools.cached(cachetools.TTLCache(maxsize=1024, ttl=1))
def read_gcs_file(gcs_client: storage.Client, gsurl: str) -> str:
    """
    Read a GCS object as a string

    Args:
        gcs_client:  GCS client
        gsurl: GCS URI for object to read in gs://bucket/path/to/object format
    Returns:
        str
    """
    blob = storage.Blob.from_string(gsurl)
    return blob.download_as_bytes(client=gcs_client).decode('UTF-8')


def read_gcs_file_if_exists(gcs_client: storage.Client,
                            gsurl: str) -> Optional[str]:
    """return string of gcs object contents or None if the object does not exist
    """
    try:
        return read_gcs_file(gcs_client, gsurl)
    except google.cloud.exceptions.NotFound:
        return None


# Cache bucket lookups (see reasoning in comment above)
@cachetools.cached(cachetools.TTLCache(maxsize=1024, ttl=1))
def cached_get_bucket(
    gcs_client: storage.Client,
    bucket_id: str,
) -> storage.Bucket:
    """get storage.Bucket object by bucket_id string if exists or raise
    google.cloud.exceptions.NotFound."""
    return gcs_client.get_bucket(bucket_id)


def dict_to_bq_schema(schema: List[Dict]) -> List[bigquery.SchemaField]:
    """Converts a list of dicts to list of bigquery.SchemaField for use with
    bigquery client library. Dicts must contain name and type keys.
    The dict may optionally contain a mode key."""
    default_mode = "NULLABLE"
    return [
        bigquery.SchemaField(
            x["name"],
            x["type"],
            mode=x.get("mode") if x.get("mode") else default_mode)
        for x in schema
    ]


# To be added to built in str in python 3.9
# https://www.python.org/dev/peps/pep-0616/
def removesuffix(in_str: str, suffix: str) -> str:
    """removes suffix from a string."""
    # suffix='' should not call self[:-0].
    if suffix and in_str.endswith(suffix):
        return in_str[:-len(suffix)]
    return in_str[:]


def recursive_update(original: Dict, update: Dict, in_place: bool = False):
    """
    return a recursively updated dictionary.

    Note, lists will be completely overwritten by value in update if there is a
    conflict.

    original: (dict) the base dictionary
    update:  (dict) the dictionary of updates to apply on original
    in_place: (bool) if true then original will be mutated in place else a new
        dictionary as a result of the update will be returned.
    """
    out = original if in_place else copy.deepcopy(original)

    for key, value in update.items():
        if isinstance(value, dict):
            out[key] = recursive_update(out.get(key, {}), value)
        else:
            out[key] = value
    return out
