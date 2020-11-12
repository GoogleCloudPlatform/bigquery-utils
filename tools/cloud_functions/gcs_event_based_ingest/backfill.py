# Copyright 2020 Google LLC
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
""" Command Line utility for backfilling gcs_ocn_bq_ingest cloud function
"""
import concurrent.futures
import logging
import os
import pprint
import sys
from argparse import ArgumentParser, Namespace
from typing import Dict, Iterator, List

from google.api_core.client_info import ClientInfo
from google.cloud import storage

import gcs_ocn_bq_ingest  # pylint: disable=import-error

CLIENT_INFO = ClientInfo(user_agent="google-pso-tool/bq-severless-loader")

os.environ["FUNCTION_NAME"] = "backfill-cli"


def find_blobs_with_suffix(
    gcs_cli: storage.Client,
    prefix: str,
    suffix: str = "_SUCCESS",
) -> Iterator[storage.Blob]:
    """
    Find GCS blobs with a given suffix.

    :param gcs_cli:  storage.Client
    :param prefix: A GCS prefix to search i.e. gs://bucket/prefix/to/search
    :param suffix: A suffix in blob name to match
    :return:  Iterable of blobs matching the suffix.
    """
    bucket_name, prefix = gcs_ocn_bq_ingest.main.parse_gcs_url(prefix)  # noqa
    bucket: storage.Bucket = gcs_cli.lookup_bucket(bucket_name)
    # filter passes on scalability / laziness advantages of iterator.
    return filter(lambda blob: blob.name.endswith(suffix),
                  bucket.list_blobs(prefix=prefix))


def main(args: Namespace):
    """main entry point for backfill CLI."""
    gcs_cli: storage.Client = storage.Client(client_info=CLIENT_INFO)
    ps_cli = None
    suffix = args.success_filename
    if args.mode == "NOTIFICATIONS":
        if not args.pubsub_topic:
            raise ValueError("when passing mode=NOTIFICATIONS"
                             "you must also pass pubsub_topic.")
        # import is here because this utility can be used without
        # google-cloud-pubsub dependency in LOCAL mode.
        # pylint: disable=import-outside-toplevel
        from google.cloud import pubsub
        ps_cli = pubsub.PublisherClient()

    # These are all I/O bound tasks so use Thread Pool concurrency for speed.
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_gsurl = {}
        for blob in find_blobs_with_suffix(gcs_cli, args.gcs_path, suffix):
            if ps_cli:
                # kwargs are message attributes
                # https://googleapis.dev/python/pubsub/latest/publisher/index.html#publish-a-message
                logging.info("sending pubsub message for: %s",
                             f"gs://{blob.bucket.name}/{blob.name}")
                future_to_gsurl[executor.submit(
                    ps_cli.publish,
                    args.pubsub_topic,
                    b'',    # cloud function ignores message body
                    bucketId=blob.bucket.name,
                    objectId=blob.name,
                    _metaInfo="this message was submitted with "
                    "gcs_ocn_bq_ingest backfill.py utility"
                )] = f"gs://{blob.bucket.name}/{blob.name}"
            else:
                logging.info("running  cloud function locally for: %s",
                             f"gs://{blob.bucket.name}/{blob.name}")
                future_to_gsurl[executor.submit(
                    gcs_ocn_bq_ingest.main.main,
                    {
                        "attributes": {
                            "bucketId": blob.bucket.name,
                            "objectId": blob.name
                        }
                    },
                    None,
                )] = f"gs://{blob.bucket.name}/{blob.name}"
        exceptions: Dict[str, Exception] = dict()
        for future in concurrent.futures.as_completed(future_to_gsurl):
            gsurl = future_to_gsurl[future]
            try:
                future.result()
            except Exception as err:  # pylint: disable=broad-except
                logging.error("Error processing %s: %s", gsurl, err)
                exceptions[gsurl] = err
        if exceptions:
            raise RuntimeError("The following errors were encountered:\n" +
                               pprint.pformat(exceptions))


def parse_args(args: List[str]) -> Namespace:
    """argument parser for backfill CLI"""
    parser = ArgumentParser(
        description="utility to backfill success file notifications "
                    "or run the cloud function locally in concurrent threads.")

    parser.add_argument(
        "--gcs_path",
        "-p",
        help="GCS path (e.g. gs://bucket/prefix/to/search/)to search for "
        "existing _SUCCESS files",
        required=True,
    )

    parser.add_argument(
        "--mode",
        "-m",
        help="How to perform the backfill: LOCAL run cloud function main"
        " method locally (in concurrent threads) or NOTIFICATIONS just push"
        " notifications to Pub/Sub for a deployed version of the cloud function"
        " to pick up. Default is NOTIFICATIONS.",
        required=False,
        type=str.upper,
        choices=["LOCAL", "NOTIFICATIONS"],
        default="NOTIFICATIONS",
    )

    parser.add_argument(
        "--pubsub_topic",
        "--topic",
        "-t",
        help="Pub/Sub notifications topic to post notifications for. "
        "i.e. projects/{PROJECT_ID}/topics/{TOPIC_ID} "
        "Required if using NOTIFICATIONS mode.",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--success_filename",
        "-f",
        help="Overide the default success filename '_SUCCESS'",
        required=False,
        default="_SUCCESS",
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    main(parse_args(sys.argv))
