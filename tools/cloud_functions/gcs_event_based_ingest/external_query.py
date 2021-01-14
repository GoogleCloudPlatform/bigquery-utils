# Copyright 2021 Google LLC
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
"""Command Line utility for dry running BigQuery queries that reference
temporary external tables over data in GCS.
"""
import argparse
import json
import logging
import sys
from typing import List

from google.cloud import bigquery
from google.cloud import storage

import gcs_ocn_bq_ingest.common.utils  # pylint: disable=import-error


def main(args: argparse.Namespace):
    """main entry point for dry run external CLI."""
    bq_client: bigquery.Client = bigquery.Client()
    gcs_client: storage.Client = storage.Client()
    gsurl = None
    if args.external_config.startswith("gs://"):
        gsurl = args.external_config
        external_config = bigquery.ExternalConfig.from_api_repr(
            gcs_ocn_bq_ingest.common.utils.read_gcs_file(gcs_client, gsurl))
    else:
        with open(args.external_config, 'r') as external_config_file:
            external_config = bigquery.ExternalConfig.from_api_repr(
                json.load(external_config_file))

    if (not external_config.source_uris
            or external_config.source_uris == ["REPLACEME"]):
        if gsurl:
            parent_gsurl = "/".join(gsurl.split("/")[:-1])
            external_config.source_uris = f"{parent_gsurl}/*"
        else:
            # need a source uri that expands to some files so use public uri
            external_config.source_uris = [
                "gs://gcp-public-data-landsat/LC08/PRE/063/046/"
                "LC80630462016136LGN00/*"
            ]
    job_config: bigquery.QueryJobConfig = bigquery.QueryJobConfig()
    job_config.table_definitions = {'temp_ext': external_config}
    job_config.dry_run = args.dry_run
    job: bigquery.QueryJob
    if args.query.startswith("gs://"):
        gsurl = args.query
        job = bq_client.query(gcs_ocn_bq_ingest.common.utils.read_gcs_file(
            gcs_client, gsurl),
                              job_config=job_config)
    else:
        with open(args.query, 'r') as query_file:
            job = bq_client.query(query_file.read(), job_config=job_config)
    if not args.dry_run:
        job.result()
        print(f"query job {job.job_id} complete")
        print(job.to_api_repr())
    else:
        logging.info(f"successful dry run of {args.query} "
                     f"with temp_ext = {args.external_config}")


def parse_args(args: List[str]) -> argparse.Namespace:
    """argument parser for backfill CLI"""
    parser = argparse.ArgumentParser(
        description="utility to dry run external queries.")

    parser.add_argument(
        "--query",
        "-q",
        help="path to file containing the query",
        required=True,
    )

    parser.add_argument(
        "--external-config",
        "-e",
        help="path to file containing external table definition",
        required=True,
    )

    parser.add_argument("--dry-run",
                        "-d",
                        help="perform a dry run of the query",
                        action='store_true')

    return parser.parse_args(args)


if __name__ == "__main__":
    main(parse_args(sys.argv))
