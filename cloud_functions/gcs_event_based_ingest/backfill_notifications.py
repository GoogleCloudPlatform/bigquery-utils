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
"""unit tests for gcs_ocn_bq_ingest"""
import os
import sys
from argparse import ArgumentParser, Namespace
from typing import List


sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
from gcs_ocn_bq_ingest.main import _parse_gcs_url
from google.cloud import storage, pubsub


def main(args: Namespace):
    gcs = storage.Client()
    ps = pubsub.Client()

def parse_args(args: List[str]):
    parser = ArgumentParser(
        description="utility to backfill success file notifications."
    )

    parser.add_argument(
        "--gcs_bucket",
        description="GCS bucket to seach for existing _SUCCESS files",
        required=True,
    )

    parser.add_argument(
        "--pubsub_topic",
        description="Pub/Sub notifications topic to post notifications for",
        required=True,
    )

    parser.add_argument(
        "--prefix",
        description="Prefix in gcs to search for success files",
        required=False,
        default=None,
    )

    parser.add_argument(
        "--success_filename",
        description="Overide the default success filename '_SUCCESS'",
        required=False,
        default="_SUCCESS",
    )
    return parser.parse_args(args)


if __name__ == "__main__":
    main(parse_args(sys.argv))
