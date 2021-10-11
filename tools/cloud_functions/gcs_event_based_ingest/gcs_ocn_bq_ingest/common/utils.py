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
"""Contains utility methods used by the BQIngest process
"""
import collections
import collections.abc
import copy
import fnmatch
import json
import os
import pathlib
import pprint
import re
import sys
import time
import uuid
from typing import Any, Deque, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import cachetools
import google.api_core
import google.api_core.client_info
import google.api_core.exceptions
import google.cloud.exceptions
from google.cloud import bigquery
from google.cloud import storage

from . import constants  # pylint: disable=no-name-in-module,import-error
from . import exceptions  # pylint: disable=no-name-in-module,import-error
from . import logging  # pylint: disable=no-name-in-module,import-error


def external_query(  # pylint: disable=too-many-arguments
        gcs_client: storage.Client, bq_client: bigquery.Client, gsurl: str,
        query: str, job_id: str, table: bigquery.TableReference):
    """Load from query over external table from GCS.

    This hinges on a SQL query defined in GCS at _config/*.sql and
    an external table definition
    _config/{constants.BQ_EXTERNAL_TABLE_CONFIG_FILENAME} (otherwise will assume
    PARQUET external table)
    """
    external_table_config = read_gcs_file_if_exists(
        gcs_client,
        f"{gsurl}_config/{constants.BQ_EXTERNAL_TABLE_CONFIG_FILENAME}")
    if not external_table_config:
        external_table_config = look_for_config_in_parents(
            gcs_client, gsurl, constants.BQ_EXTERNAL_TABLE_CONFIG_FILENAME)
    if external_table_config:
        external_table_def = json.loads(external_table_config)
    else:
        print(f" {gsurl}_config/{constants.BQ_EXTERNAL_TABLE_CONFIG_FILENAME} "
              f"not found in parents of {gsurl}. "
              "Falling back to default PARQUET external table: "
              f"{json.dumps(constants.DEFAULT_EXTERNAL_TABLE_DEFINITION)}")
        external_table_def = constants.DEFAULT_EXTERNAL_TABLE_DEFINITION
    print(
        json.dumps(
            dict(message="Found external table definition.",
                 table=table.to_api_repr(),
                 external_table_def=external_table_def)))
    # Reduce the amount of sourceUris by using wildcards with common
    # prefixes. This is done to keep the cloud logging audit metadata
    # below 100k in size, otherwise the metadata is omitted in the event.
    source_uris_with_wildcards = compact_source_uris_with_wildcards(
        flatten2dlist(get_batches_for_gsurl(gcs_client, gsurl)))
    # This may cause an issue if >10,000 files.
    external_table_def["sourceUris"] = source_uris_with_wildcards
    # Check if hivePartitioningOptions
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#hivepartitioningoptions
    # is set in external.json file
    if external_table_def.get("hivePartitioningOptions"):
        external_table_def["hivePartitioningOptions"] = {
            "mode":
                external_table_def["hivePartitioningOptions"].get("mode")
                or "AUTO",
            "sourceUriPrefix":
                get_hive_partitioning_source_uri_prefix(
                    external_table_def["sourceUris"][0])
        }
    external_config = bigquery.ExternalConfig.from_api_repr(external_table_def)
    job_config = bigquery.QueryJobConfig(
        table_definitions={"temp_ext": external_config}, use_legacy_sql=False)

    # drop partition decorator if present.
    table_id = table.table_id.split("$")[0]
    # similar syntax to str.format but doesn't require escaping braces
    # elsewhere in query (e.g. in a regex)
    rendered_query = query.replace(
        "{dest_dataset}", f"`{table.project}`.{table.dataset_id}").replace(
            "{dest_table}", table_id)
    job: bigquery.QueryJob = bq_client.query(rendered_query,
                                             job_config=job_config,
                                             job_id=job_id)
    logging.log_bigquery_job(job, table,
                             f"Submitted asynchronous query job: {job.job_id}")
    start_poll_for_errors = time.monotonic()
    # Check if job failed quickly
    while time.monotonic(
    ) - start_poll_for_errors < constants.WAIT_FOR_JOB_SECONDS:
        job.reload(client=bq_client)
        if job.state == "DONE":
            check_for_bq_job_and_children_errors(bq_client, job, table)
            return
        time.sleep(constants.JOB_POLL_INTERVAL_SECONDS)


def compact_source_uris_with_wildcards(source_uris: List[str]):
    """Given a list of source URIs, combine URIs using common prefixes and wildcards.

    For example, given the following list:
      ["gs://bucket/batch/file1.csv", "gs://bucket/batch/file2.csv", "gs://bucket/batch/file3.csv"]

    Return the following list:
      ["gs://bucket/batch/*.csv"]
    """
    source_uris_with_wildcards = set()
    for source_uri in source_uris:
        file_name = os.path.basename(source_uri)
        if len(file_name.split('.')) > 1:
            # If file extension is present, use it with a wildcard
            # (e.g. *.csv)
            source_uris_with_wildcards.add(
                f"{os.path.dirname(source_uri)}/*.{file_name.split('.')[-1]}")
        else:
            # If no file extension is present, just add the source_uri
            # as-is in order to avoid picking up other control files
            # (e.g. _SUCCESS) or empty files.
            source_uris_with_wildcards.add(source_uri)
    return list(source_uris_with_wildcards)


def get_hive_partitioning_source_uri_prefix(gcs_uri: str):
    """Given a gcs uri, parse out the hive partitioning source uri prefix

    This function will look for sourceUriPrefix formats as described in
    https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#hivepartitioningoptions
    In short, it will return the gcs uri prefix that comes before any path element which
    contains a key,value pair (equal sign between two words).
    """
    source_uri_prefix_regex = r'^(?P<sourceUriPrefix>gs://.*?)/[\w\-]+=[\w\-]*/[\w\-\*]+'
    match = re.compile(source_uri_prefix_regex).match(gcs_uri)
    if not match:
        raise exceptions.HiveSourceUriPrefixRegexMatchException(
            f"could not determine a source uri prefix for path: {gcs_uri}"
            "because it did not contain a match for regex: "
            f"{source_uri_prefix_regex}")
    return match.groupdict().get("sourceUriPrefix")


def flatten2dlist(arr: List[List[Any]]) -> List[Any]:
    """Flatten list of lists to flat list of elements"""
    return [j for i in arr for j in i]


def load_batches(gcs_client: storage.Client, bq_client: bigquery.Client,
                 gsurl: str, load_config: bigquery.LoadJobConfig, job_id: str,
                 table: bigquery.TableReference):
    """orchestrate 1 or more load jobs based on number of URIs and total byte
    size of objects at gsurl"""
    batches = get_batches_for_gsurl(gcs_client, gsurl)
    jobs: List[Tuple[bigquery.TableReference, bigquery.LoadJob]] = []
    for batch in batches:
        # None is passed to destination parameter below because the load_config
        # object contains the destination information
        job: bigquery.LoadJob = bq_client.load_table_from_uri(
            batch, None, job_config=load_config, job_id=job_id)
        jobs.append((table, job))
        logging.log_bigquery_job(
            job, table,
            f"Submitted asynchronous bigquery load job: {job.job_id}")
    start_poll_for_errors = time.monotonic()
    # Check if job failed quickly
    while time.monotonic(
    ) - start_poll_for_errors < constants.WAIT_FOR_JOB_SECONDS:
        # Check if job failed quickly
        for table_ref, job in jobs:
            job.reload(client=bq_client)
            check_for_bq_job_and_children_errors(bq_client, job, table_ref)
        time.sleep(constants.JOB_POLL_INTERVAL_SECONDS)


def _get_parent_config_file(storage_client, config_filename, bucket, path):
    bkt = storage_client.lookup_bucket(bucket)
    config_dir_name = "_config"
    parent_path = pathlib.Path(path).parent
    config_path = parent_path / config_dir_name
    config_file_path = config_path / config_filename
    # Handle wild card (to support bq transform sql with different names).
    if "*" in config_filename:
        matches: List[storage.Blob] = list(
            filter(lambda blob: fnmatch.fnmatch(blob.name, config_filename),
                   bkt.list_blobs(prefix=config_path)))
        if matches:
            if len(matches) > 1:
                raise RuntimeError(
                    f"Multiple matches for gs://{bucket}/{config_file_path}")
            return read_gcs_file_if_exists(storage_client,
                                           f"gs://{bucket}/{matches[0].name}")
        return None
    return read_gcs_file_if_exists(storage_client,
                                   f"gs://{bucket}/{config_file_path}")


def look_for_config_in_parents(storage_client: storage.Client, gsurl: str,
                               config_filename: str) -> Optional[str]:
    """look in parent directories for _config/config_filename"""
    print(
        f"Looking for {config_filename} in any parent _config/ directory for gsurl: {gsurl}"
    )
    blob: storage.Blob = storage.Blob.from_string(gsurl)
    bucket_name = blob.bucket.name
    obj_path = blob.name
    parts = removesuffix(obj_path, "/").split("/")

    def _get_parent_config(path):
        return _get_parent_config_file(storage_client, config_filename,
                                       bucket_name, path)

    config = None
    while parts:
        if config is not None:
            return config
        config = _get_parent_config("/".join(parts))
        parts.pop()
    return config


def construct_config(storage_client: storage.Client, blob: storage.Blob,
                     config_filename: str) -> Dict:
    """
    merge dictionaries for configs in parent directories.
    The configs closest to gsurl should take precedence.
    """
    gsurl = removesuffix(f"gs://{blob.bucket.name}/{blob.name}",
                         constants.SUCCESS_FILENAME)
    blob = storage.Blob.from_string(gsurl)
    bucket_name = blob.bucket.name
    obj_path = blob.name
    parts = removesuffix(obj_path, "/").split("/")

    def _get_parent_config(path):
        return _get_parent_config_file(storage_client, config_filename,
                                       bucket_name, path)

    config_q: Deque[Dict[str, Any]] = collections.deque()
    if config_filename == constants.BQ_LOAD_CONFIG_FILENAME:
        config_q.append(constants.BASE_LOAD_JOB_CONFIG)
    while parts:
        config = _get_parent_config("/".join(parts))
        if config:
            print(f"found config: {'/'.join(parts)}")
            config_q.append(json.loads(config))
        parts.pop()
    merged_config: Dict = {}
    while config_q:
        recursive_update(merged_config, config_q.popleft(), in_place=True)
    print(f"merged_config for {config_filename}: {json.dumps(merged_config)}")
    if merged_config == constants.BASE_LOAD_JOB_CONFIG:
        print("falling back to default CSV load job config. "
              "Did you forget load.json?")
        return {"load": constants.DEFAULT_LOAD_JOB_CONFIG}
    if config_filename == constants.BQ_LOAD_CONFIG_FILENAME:
        return {"load": merged_config}
    # retuning any other config file that doesn't have the same name as
    # constants.BQ_LOAD_CONFIG_FILENAME (default load.json)
    return merged_config


def get_batches_for_gsurl(gcs_client: storage.Client,
                          gsurl: str,
                          recursive=True) -> List[List[str]]:
    """
    This function creates batches of GCS uris for a given gsurl.
    By default, it will recursively search for blobs in all sub-folders of the
    given gsurl.
    The function will ignore uris of objects which match the following:
      - filenames which are present in constants.ACTION_FILENAMES
      - filenames that start with a dot (.)
      - _bqlock file created for ordered loads
      - filename contains any constant.SPECIAL_GCS_DIRECTORY_NAMES in their path
    returns an Array of their batches
    (one batch has an array of multiple GCS uris)
    """
    batches: List[List[str]] = []
    parsed_url = urlparse(gsurl)
    bucket_name: str = parsed_url.netloc
    prefix_path: str = parsed_url.path.lstrip('/')

    bucket: storage.Bucket = cached_get_bucket(gcs_client, bucket_name)
    folders: Set[str] = get_folders_in_gcs_path_prefix(gcs_client,
                                                       bucket,
                                                       prefix_path,
                                                       recursive=recursive)
    folders.add(prefix_path)
    print(
        json.dumps(
            dict(message="Searching for blobs to load in"
                 " prefix path and sub-folders",
                 search_folders=list(folders),
                 severity="INFO")))
    blobs: List[storage.Blob] = []
    for folder in folders:
        blobs += (list(
            gcs_client.list_blobs(bucket, prefix=folder, delimiter="/")))
    cumulative_bytes = 0
    max_batch_size = int(
        os.getenv("MAX_BATCH_BYTES", constants.DEFAULT_MAX_BATCH_BYTES))
    batch: List[str] = []
    for blob in blobs:
        # The following blobs will be ignored:
        #   - filenames which are present in constants.ACTION_FILENAMES
        #   - filenames that start with a dot (.)
        #   - _bqlock file created for ordered loads
        #   - filenames with constants.SPECIAL_GCS_DIRECTORY_NAMES in their path
        if (os.path.basename(blob.name) not in constants.ACTION_FILENAMES and
                not os.path.basename(blob.name).startswith(".") and
                os.path.basename(blob.name) != "_bqlock" and
                not any(blob_dir_name in constants.SPECIAL_GCS_DIRECTORY_NAMES
                        for blob_dir_name in blob.name.split('/'))):
            if blob.size == 0:  # ignore empty files
                print(f"ignoring empty file: gs://{bucket.name}/{blob.name}")
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

    print(
        json.dumps(
            dict(message="Logged batches of blobs to load in jsonPayload.",
                 batches=batches)))

    if len(batches) > 1:
        print(f"split into {len(batches)} batches.")
    elif len(batches) < 1:
        raise google.api_core.exceptions.NotFound(
            f"No files to load at {gsurl}!")
    return batches


def get_folders_in_gcs_path_prefix(gcs_client,
                                   bucket,
                                   prefix_path,
                                   recursive=True):
    """
    This function lists all folders in a given GCS path using a more
    efficient prefix filtering method so it only lists objects in a bucket
    with a given prefix instead of listing all the objects in a bucket.
    Inspiration for this method came from:
    https://github.com/googleapis/google-cloud-python/issues/920#issuecomment
    -326125992 :param gcs_client: :param bucket: :param prefix_path: :param
    recursive: Whether to recursively search for folders :return: list of GCS
    URIs
    """

    if (prefix_path is not None and not prefix_path.endswith('/') and
            prefix_path != ''):
        prefix_path = f"{prefix_path}/"
    resp = gcs_client.list_blobs(bucket, prefix=prefix_path, delimiter='/')
    # Iterate through response pages to retrieve only
    # the gcs folder names (the file prefixes)
    folders = set()
    prefixes = set()
    for page in resp.pages:
        prefixes.update(page.prefixes)
    # Check for folders within folders
    for prefix in prefixes:
        folders.add(prefix)
        if recursive:
            folders.update(
                get_folders_in_gcs_path_prefix(gcs_client, bucket, prefix))
    return folders


def parse_notification(notification: dict) -> Tuple[str, str]:
    """validates notification payload
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
        # notification is GCS Object resource from Cloud Functions trigger
        # https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        return notification["bucket"], notification["name"]
    if notification.get("attributes"):
        # notification is Pub/Sub message.
        try:
            attributes = notification["attributes"]
            return attributes["bucketId"], attributes["objectId"]
        except KeyError:
            raise exceptions.UnexpectedTriggerException(
                "Issue with Pub/Sub message, did not contain expected "
                f"attributes: 'bucketId' and 'objectId': {notification}"
            ) from KeyError
    raise exceptions.UnexpectedTriggerException(
        "Cloud Function received unexpected trigger: "
        f"{notification} "
        "This function only supports direct Cloud Functions "
        "Background Triggers or Pub/Sub storage notifications "
        "as described in the following links: "
        "https://cloud.google.com/storage/docs/pubsub-notifications "
        "https://cloud.google.com/functions/docs/tutorials/storage")


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


# cache lookups against GCS API for 1 second as buckets have update
# limit of once per second and we might do several of the same lookup during
# the functions lifetime. This should improve performance by eliminating
# unnecessary API calls.
# https://cloud.google.com/storage/quotas
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
def removeprefix(in_str: str, prefix: str) -> str:
    """remove string prefix"""
    if in_str.startswith(prefix):
        return in_str[len(prefix):]
    return in_str[:]


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


def handle_duplicate_notification(
    gcs_client: storage.Client,
    blob_to_claim: storage.Blob,
):
    """
    Need to handle potential duplicate Pub/Sub notifications.
    To achieve this we will drop an empty "claimed" file that indicates
    an invocation of this cloud function has picked up the success file
    with a certain creation timestamp. This will support republishing the
    success file as a mechanism of re-running the ingestion while avoiding
    duplicate ingestion due to multiple Pub/Sub messages for a success file
    with the same creation time.
    """
    blob_to_claim.reload(client=gcs_client)
    created_unix_timestamp = blob_to_claim.time_created.timestamp()

    basename = os.path.basename(blob_to_claim.name)
    claim_blob: storage.Blob = blob_to_claim.bucket.blob(
        blob_to_claim.name.replace(
            basename, f"_claimed_{basename}_created_at_"
            f"{created_unix_timestamp}"))
    try:
        claim_blob.upload_from_string("",
                                      if_generation_match=0,
                                      client=gcs_client)
    except google.api_core.exceptions.PreconditionFailed as err:
        blob_to_claim.reload(client=gcs_client)
        raise exceptions.DuplicateNotificationException(
            f"gs://{blob_to_claim.bucket.name}/{blob_to_claim.name} appears "
            "to already have been claimed for created timestamp: "
            f"{created_unix_timestamp}."
            "This means that another invocation of this cloud function has "
            "claimed the work to be one for this file. "
            "This may be due to a rare duplicate delivery of the Pub/Sub "
            "storage notification.") from err


def remove_blob_quietly(
    gcs_client: storage.Client,
    blob: storage.Blob,
):
    """
    Removes a blob and eats the error if it doesn't exist.
    """
    try:
        blob.delete(client=gcs_client)
    except google.api_core.exceptions.NotFound:
        print(f"Attempted to delete {blob.name=} "
              f"but the file wasn't found.")


def get_table_from_load_job_config(config: bigquery.LoadJobConfig):
    """
    The BigQuery python library does not currently expose destinationTable
    as a property in the LoadJobConfig class. Because of this, you have to
    convert the LoadJobConfig object to the API representation and then
    extract the dictionary's value for key: destinationTable.

    :param config: bigquery.LoadJobConfig
    :return: bigquery.TableReference
    """
    print(f"Retrieving table for LoadJobConfig: {config.to_api_repr()}")
    if config.to_api_repr().get('load'):
        config = config.to_api_repr().get('load')
        if config.get('destinationTable'):
            project_id = config.get('destinationTable').get('projectId')
            dataset_id = config.get('destinationTable').get('datasetId')
            table_id = config.get('destinationTable').get('tableId')
            return bigquery.TableReference.from_string(
                f"{project_id}.{dataset_id}.{table_id}")
    return None


@cachetools.cached(cachetools.LRUCache(maxsize=1024))
def get_table_prefix(gcs_client: storage.Client, blob: storage.Blob) -> str:
    """Find the table prefix for a object_id based on the destination regex.
    Args:
        gcs_client: storage.Client
        blob: storage.Blob to parse
    Returns:
        str: table prefix
    """
    basename = os.path.basename(blob.name)
    if basename in {
            constants.BACKFILL_FILENAME,
            constants.START_BACKFILL_FILENAME,
            "_bqlock",
    }:
        # These files will not match the regex and always should appear at the
        # table level.
        return removesuffix(blob.name, f"/{basename}")
    load_config = construct_config(
        gcs_client, blob, constants.BQ_LOAD_CONFIG_FILENAME).get('load')
    if load_config:
        destination_regex = load_config.get('destinationRegex',
                                            constants.DESTINATION_REGEX)
        print(f"Retrieved DESTINATION_REGEX: {destination_regex}")
        match = re.compile(destination_regex).match(
            blob.name.replace("/_backlog/", "/"))
        if not match:
            raise exceptions.DestinationRegexMatchException(
                f"could not determine table prefix for object id: {blob.name}"
                "because it did not contain a match for destination_regex: "
                f"{destination_regex}")
        table_group_index = match.re.groupindex.get("table")
        if table_group_index:
            table_level_index = match.regs[table_group_index][1]
            table_prefix = blob.name[:table_level_index].rstrip('/')
            print(f"{table_prefix=}")
            return table_prefix
    raise exceptions.DestinationRegexMatchException(
        f"could not determine table prefix for object id: {blob.name}"
        "because it did not contain a match for the table capturing group "
        f"in destination regex: {constants.DESTINATION_REGEX}")


def get_next_backlog_item(
    gcs_client: storage.Client,
    bkt: storage.Bucket,
    table_prefix: str,
) -> Optional[storage.Blob]:
    """
    Get next blob in the backlog if the backlog is not empty.

    Args:
        gcs_client: storage.Client
        bkt: storage.Bucket that this cloud functions is ingesting data for.
        table_prefix: the prefix for the table whose backlog should be checked.

    Returns:
        storage.Blob: pointer to a SUCCESS file in the backlog
    """
    backlog_blobs = gcs_client.list_blobs(bkt,
                                          prefix=f"{table_prefix}/_backlog/")
    # Backlog items will be lexicographically sorted
    # https://cloud.google.com/storage/docs/json_api/v1/objects/list
    for blob in backlog_blobs:
        return blob  # Return first item in iterator
    return None


def remove_oldest_backlog_item(
    gcs_client: storage.Client,
    bkt: storage.Bucket,
    table_prefix: str,
) -> bool:
    """
    Remove the oldest pointer in the backlog if the backlog is not empty.

    Args:
        gcs_client: storage.Client
        bkt: storage.Bucket that this cloud functions is ingesting data for.
        table_prefix: the prefix for the table whose backlog should be checked.

    Returns:
        bool: True if we removed the oldest blob. False if the backlog was
        empty.
    """
    backlog_blobs = gcs_client.list_blobs(bkt,
                                          prefix=f"{table_prefix}/_backlog/")
    # Backlog items will be lexicographically sorted
    # https://cloud.google.com/storage/docs/json_api/v1/objects/list
    blob: storage.Blob
    for blob in backlog_blobs:
        blob.delete(client=gcs_client)
        return True  # Return after deleteing first blob in the iterator
    return False


def check_for_bq_job_and_children_errors(
        bq_client: bigquery.Client, job: Union[bigquery.LoadJob,
                                               bigquery.QueryJob],
        table: Optional[bigquery.TableReference]):
    """checks if BigQuery job (or children jobs in case of multi-statement sql)
    should be considered failed because there were errors or the query affected
    no rows while FAIL_ON_ZERO_DML_ROWS_AFFECTED env var is set to True
    (this is the default).

    Args:
        bq_client: bigquery.Client
        job: Union[bigquery.LoadJob, bigquery.QueryJob] job to check for errors.
        table: bigquery.TableReference of table being loaded
    Raises:
        exceptions.BigQueryJobFailure
    """
    if job.state != "DONE":
        wait_on_bq_job_id(bq_client, job.job_id, table, 5)
    if job.error_result:
        logging.log_bigquery_job(job, table)
        # Raise any 5xx error codes
        exception: Optional[
            google.api_core.exceptions.GoogleAPICallError] = job.exception()
        if isinstance(exception, (google.api_core.exceptions.ServerError,
                                  google.api_core.exceptions.BadRequest)):
            # Raise these two exception types so that the job can be retried
            raise exception
        raise exceptions.BigQueryJobFailure(
            f"BigQuery Job {job.job_id} failed during backfill with the "
            f"following errors: {job.error_result} "
            f"{pprint.pformat(job.to_api_repr())}")
    if isinstance(job, bigquery.QueryJob):
        if (constants.FAIL_ON_ZERO_DML_ROWS_AFFECTED and
                job.statement_type in constants.BQ_DML_STATEMENT_TYPES and
                job.num_dml_affected_rows < 1):
            logging.log_bigquery_job(
                job,
                table,
                "BigQuery query job ran successfully "
                "but did not affect any rows.",
                "ERROR",
            )
            raise exceptions.BigQueryJobFailure(
                f"query job {job.job_id} ran successfully but did not "
                f"affect any rows.  {pprint.pformat(job.to_api_repr())}")
        for child_job in bq_client.list_jobs(parent_job=job):
            check_for_bq_job_and_children_errors(bq_client, child_job, table)


def wait_on_bq_job_id(bq_client: bigquery.Client,
                      job_id: str,
                      table: bigquery.TableReference,
                      polling_timeout: int,
                      polling_interval: int = 1) -> bool:
    """"
    Wait for a BigQuery Job ID to complete.

    Args:
        bq_client: bigquery.Client
        job_id: str the BQ job ID to wait on
        table: bigquery.TableReference of table being loaded
        polling_timeout: int number of seconds to poll this job ID
        polling_interval: frequency to query the job state during polling
    Returns:
        bool: if the job ID has finished successfully. True if DONE without
        errors, False if RUNNING or PENDING
    Raises:
        exceptions.BigQueryJobFailure if the job failed.
        google.api_core.exceptions.NotFound if the job id cannot be found.
    """
    job: Union[bigquery.LoadJob, bigquery.QueryJob] = bq_client.get_job(job_id)
    start_poll = time.monotonic()
    while time.monotonic() - start_poll < (polling_timeout - polling_interval):
        if job.state == "DONE":
            check_for_bq_job_and_children_errors(bq_client, job, table)
            return True
        if job.state in {"RUNNING", "PENDING"}:
            logging.log_bigquery_job(job, table,
                                     f"Waiting on BigQuery Job {job.job_id=}")
            time.sleep(polling_interval)
        job = bq_client.get_job(job_id)
    logging.log_bigquery_job(
        job, table,
        f"Reached polling timeout waiting for bigquery job {job.job_id=}.",
        "WARN")
    return False


def wait_on_gcs_blob(gcs_client: storage.Client,
                     wait_blob: storage.Blob,
                     polling_timeout: int,
                     polling_interval: int = 1) -> bool:
    """"
    Wait for a GCS Object to exists.

    Args:
        gcs_client: storage.Client
        wait_blob: storage.Bllob the GCS to wait on.
        polling_timeout: int number of seconds to poll this job ID
        polling_interval: frequency to query the job state during polling
    Returns:
        bool: if the job ID has finished successfully. True if DONE without
        errors, False if RUNNING or PENDING
    Raises:
        exceptions.BigQueryJobFailure if the job failed.
        google.api_core.exceptions.NotFound if the job id cannot be found.
    """
    start_poll = time.monotonic()
    while time.monotonic() - start_poll < (polling_timeout - polling_interval):
        if wait_blob.exists(client=gcs_client):
            return True
        print(
            f"waiting on GCS file gs://{wait_blob.bucket.name}/{wait_blob.name}"
        )
        time.sleep(polling_interval)
    return False


def gcs_path_to_load_config_and_datasource_name(
    gcs_client: storage.Client, blob: storage.Blob,
    default_project: Optional[str]
) -> Tuple[bigquery.LoadJobConfig, Optional[str]]:
    """extract bigquery load config and load data source name

    This function will search for destination table information in two places:
     - GCS path (table info extracted via regex)
     - BQ_LOAD_CONFIG_FILENAME (table info stored as TableReference JSON inside
       destinationTable dict)
       See: https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationload
            https://cloud.google.com/bigquery/docs/reference/rest/v2/TableReference
    It will return a tuple containing the BigQuery load job config and a datasource name if
    it is provided inside the BQ_LOAD_CONFIG_FILENAME file.
    Args:
        gcs_client: storage.Client
        blob: storage.Blob
        default_project: Optional[str]
    Returns:
        Tuple[bigquery.LoadJobConfig, Optional[str]]
    Raises:
        exceptions.DestinationRegexMatchException if the regex matched but the
         dictionary of matches does not contain a table or a dataset.
        RuntimeError if the destination regex didn't match anything in the
         GCS path
    """
    load_config = construct_config(
        gcs_client, blob, constants.BQ_LOAD_CONFIG_FILENAME).get('load')
    if load_config:
        dest_config: Dict = load_config.get('destinationTable') or {}
        dest_regex = load_config.get(
            'destinationRegex') or constants.DESTINATION_REGEX
        destination_match = re.compile(dest_regex).match(blob.name)
        if not destination_match:
            raise RuntimeError(f"Object ID {blob.name} did not match regex:"
                               f" {constants.DESTINATION_REGEX}")
        destination_details = destination_match.groupdict()
        try:
            project = dest_config.get('projectId') or os.getenv(
                "BQ_STORAGE_PROJECT", default_project)
            dataset = dest_config.get(
                'datasetId') or destination_details['dataset']
            # Use project id if it's specified in the GCS path, but only if
            # there isn't a project specified in the destinationTable dict
            # JSON config file
            if '.' in dataset and not dest_config:
                project = dataset.split('.')[0]
                dataset = dataset.split('.')[1]
            table = dest_config.get('tableId') or destination_details['table']
        except KeyError:
            raise exceptions.DestinationRegexMatchException(
                f"Object ID {blob.name} "
                f"did not match dataset and table in regex:"
                f" {constants.DESTINATION_REGEX}") from KeyError
        partition = destination_details.get('partition')
        # The f'{int(destination_details.get(key, "")):02}' logic below will
        # pad the month, day, and hour with a zero if the value is only
        # a single digit.
        year, month, day, hour = (f'{int(destination_details.get(key, "")):02}'
                                  if destination_details.get(key) else ""
                                  for key in ('yyyy', 'mm', 'dd', 'hh'))
        part_list = (year, month, day, hour)
        if not partition and any(part_list):
            partition = '$' + ''.join(filter(None, part_list))
        batch_id = destination_details.get('batch')
        labels = constants.DEFAULT_JOB_LABELS

        if batch_id:
            labels["batch-id"] = batch_id
        if partition:
            load_config['destinationTable'] = {
                'projectId': project,
                'datasetId': dataset,
                'tableId': f"{table}{partition}"
            }
        else:
            load_config['destinationTable'] = {
                'projectId': project,
                'datasetId': dataset,
                'tableId': table
            }
        # destinationRegex is a custom key that must be removed before
        # converting to https://cloud.google.com/bigquery/docs/reference/rest
        # /v2/Job#jobconfigurationload
        if load_config.get('destinationRegex'):
            load_config.pop('destinationRegex')
        data_source_name = load_config.get('dataSourceName')
        # dataSourceName is a custom user-provided key that must be removed before
        # converting to https://cloud.google.com/bigquery/docs/reference/rest
        # /v2/Job#jobconfigurationload
        if data_source_name:
            load_config.pop('dataSourceName')
        print(f"Building LoadJobConfig from {load_config=}")
        bq_load_config: bigquery.LoadJobConfig = (
            bigquery.LoadJobConfig.from_api_repr({'load': load_config}))
        bq_load_config.labels = constants.DEFAULT_JOB_LABELS
        return bq_load_config, data_source_name
    raise RuntimeError(f"No {constants.BQ_LOAD_CONFIG_FILENAME=} file"
                       f"found for {blob.name=}")


def create_job_id(success_file_path, data_source_name=None, table=None):
    """Create job id prefix with a consistent naming convention based on the
    success file path to give context of what caused this job to be submitted.
    the rules for success file name -> job id are:
    1. slashes to dashes
    2. all non-alphanumeric dash or underscore will be replaced with underscore
    Note, gcf-ingest- can be overridden with environment variable JOB_PREFIX
    3. uuid for uniqueness
    """
    if data_source_name and table:
        if len(table.table_id.split('$')) == 2:
            # This code is reached if the user has set an explicit load_data_source
            # key,value pair in the BQ_LOAD_CONFIG_FILENAME file and the GCS path has
            # partition information.
            partition_info = table.table_id.split('$')[1]
            clean_job_id = (f'{data_source_name}/'
                            f'{table.dataset_id}/'
                            f'{table.table_id.split("$")[0]}/')
            if len(partition_info) >= 4:
                clean_job_id += f'{partition_info[0:4]}/'
            if len(partition_info) >= 6:
                clean_job_id += f'{partition_info[4:6]}/'
            if len(partition_info) >= 8:
                clean_job_id += f'{partition_info[6:8]}/'
            if len(partition_info) == 10:
                clean_job_id += f'{partition_info[8:10]}/'
        else:
            # This code is reached if the user has set an explicit load_data_source
            # key,value pair in the BQ_LOAD_CONFIG_FILENAME file but the GCS path
            # and regex does NOT have any partition information.
            clean_job_id = (f'{data_source_name}/'
                            f'{table.dataset_id}/'
                            f'{table.table_id}/')
        clean_job_id = clean_job_id.replace('-',
                                            '_').replace('/',
                                                         '-').replace('$', '')
        clean_job_id = os.getenv('JOB_PREFIX',
                                 constants.DEFAULT_JOB_PREFIX) + clean_job_id
        clean_job_id += str(uuid.uuid4())
    else:
        clean_job_id = os.getenv('JOB_PREFIX', constants.DEFAULT_JOB_PREFIX)
        clean_job_id += re.compile(constants.NON_BQ_JOB_ID_REGEX).sub(
            '_', success_file_path.replace('/', '-'))
        # add uniqueness in case we have to "re-process" a success file that is
        # republished (e.g. to fix a bad batch of data) or handle multiple load jobs
        # for a single success file.
        clean_job_id += str(uuid.uuid4())
    # Make sure job id isn't too long (1024 chars max), but also leave 3 chars of space so that if a job fails
    # we can add a retry counter suffix to the original job_id.
    # For example, if 'some_job_id' fails, then on the third retry we'd see the following job id:
    #   some_job_id_03
    # where _03 means the third retry attempt.
    #
    # Source for job id max length: https://cloud.google.com/bigquery/docs/running-jobs#generate-jobid
    return clean_job_id[:1021]


def handle_bq_lock(gcs_client: storage.Client,
                   lock_blob: storage.Blob,
                   next_job_id: Optional[str],
                   table: bigquery.TableReference,
                   retry_attempt_cnt: Optional[int] = None):
    """Reclaim the lock blob for the new job id (in-place) or delete the lock
    blob if next_job_id is None."""
    try:
        if next_job_id:
            lock_blob_contents = json.dumps(
                dict(job_id=next_job_id,
                     table=table.to_api_repr(),
                     retry_attempt_cnt=retry_attempt_cnt))
            logging.log_with_table(
                table,
                f"Writing the following content to lock_blob {lock_blob.name}:"
                f" {dict(job_id=next_job_id, table=table.to_api_repr(), retry_attempt_cnt=retry_attempt_cnt)}"
            )
            if lock_blob.exists(client=gcs_client):
                lock_blob.upload_from_string(
                    lock_blob_contents,
                    if_generation_match=lock_blob.generation,
                    client=gcs_client)
            else:  # This happens when submitting the first job in the backlog
                lock_blob.upload_from_string(
                    lock_blob_contents,
                    if_generation_match=0,  # noqa: E126
                    client=gcs_client)
        else:
            logging.log_with_table(
                table, "releasing lock at: "
                f"gs://{lock_blob.bucket.name}/{lock_blob.name}")
            lock_blob.delete(
                if_generation_match=lock_blob.generation,
                client=gcs_client,
            )
    except (google.api_core.exceptions.PreconditionFailed,
            google.api_core.exceptions.NotFound) as err:
        if isinstance(err, google.api_core.exceptions.PreconditionFailed):
            raise exceptions.BacklogException(
                f"The lock at gs://{lock_blob.bucket.name}/{lock_blob.name} "
                f"was changed by another process.") from err
        logging.log_with_table(
            table, "Tried deleting a lock blob that was either already deleted "
            "or never existed.")


def apply(
    gcs_client: storage.Client,
    bq_client: bigquery.Client,
    success_blob: storage.Blob,
    lock_blob: Optional[storage.Blob],
    job_id: str,
):
    # pylint: disable=too-many-locals
    """
    Apply an incremental batch to the target BigQuery table via an asynchronous
    load job or external query.

    Args:
        gcs_client: storage.Client
        bq_client: bigquery.Client
        success_blob: storage.Blob the success file whose batch should be
            applied.
        lock_blob: storage.Blob _bqlock blob to acquire for this job.
        job_id: str
    """
    handle_duplicate_notification(gcs_client, success_blob)
    bkt = success_blob.bucket
    gsurl = removesuffix(f"gs://{bkt.name}/{success_blob.name}",
                         constants.SUCCESS_FILENAME)
    load_config, data_source_name = gcs_path_to_load_config_and_datasource_name(
        gcs_client, success_blob, bq_client.project)
    table = get_table_from_load_job_config(load_config)
    custom_job_id = None
    if data_source_name:
        custom_job_id = create_job_id(success_blob.name, data_source_name,
                                      table)
    if lock_blob:
        handle_bq_lock(gcs_client, lock_blob, custom_job_id or job_id, table)
    external_query_sql = look_for_config_in_parents(
        gcs_client, f"gs://{bkt.name}/{success_blob.name}", '*.sql')
    try:
        if external_query_sql:
            external_query(gcs_client, bq_client, gsurl, external_query_sql,
                           custom_job_id or job_id, table)
            return
        load_batches(gcs_client, bq_client, gsurl, load_config, custom_job_id or
                     job_id, table)
        return
    except (google.api_core.exceptions.GoogleAPIError,
            google.api_core.exceptions.ClientError) as err:
        etype, value, _ = sys.exc_info()
        msg = (f"failed to submit job {custom_job_id or job_id} for {gsurl}: "
               f"{etype.__class__.__name__}: {value}")
        blob = storage.Blob.from_string(gsurl)
        table_prefix = get_table_prefix(gcs_client, blob)
        bqlock = storage.Blob.from_string(
            f"gs://{blob.bucket.name}/{table_prefix}/_bqlock")
        # Write this error message to avoid confusion.
        handle_bq_lock(gcs_client, bqlock, msg, table)
        logging.log_api_error(table, msg, err)
        raise exceptions.BigQueryJobFailure(msg) from err
