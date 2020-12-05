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
"""Configurations for Cloud Function for loading data from GCS to BigQuery.
"""
import os

import google.api_core.client_info
import google.cloud.exceptions

# Will wait up to this polling for errors before exiting
# This is to check if job fail quickly, not to assert it succeed.
# This may not be honored if longer than cloud function timeout.
# https://cloud.google.com/functions/docs/concepts/exec#timeout
# One might consider lowering this to 1-2 seconds to lower the
# upper bound of expected execution time to stay within the free tier.
# https://cloud.google.com/functions/pricing#free_tier
WAIT_FOR_JOB_SECONDS = int(os.getenv("WAIT_FOR_JOB_SECONDS", "5"))

DEFAULT_EXTERNAL_TABLE_DEFINITION = {
    # The default must be a self describing data format
    # because autodetecting CSV /JSON schemas is likely to not match
    # expectations / assumptions of the transformation query.
    "sourceFormat": "PARQUET",
}

# Use caution when lowering the job polling rate.
# Keep in mind that many concurrent executions of this cloud function should not
# violate the 300 concurrent requests or 100 request per second.
# https://cloud.google.com/bigquery/quotas#all_api_requests
JOB_POLL_INTERVAL_SECONDS = 1

DEFAULT_JOB_LABELS = {
    "component": "event-based-gcs-ingest",
    "cloud-function-name": os.getenv("FUNCTION_NAME"),
}

BASE_LOAD_JOB_CONFIG = {
    "sourceFormat": "CSV",
    "fieldDelimiter": ",",
    "writeDisposition": "WRITE_APPEND",
    "labels": DEFAULT_JOB_LABELS,
}

# https://cloud.google.com/bigquery/quotas#load_jobs
# 15TB per BQ load job (soft limit).
DEFAULT_MAX_BATCH_BYTES = str(15 * 10**12)

# 10,000 GCS URIs per BQ load job.
MAX_SOURCE_URIS_PER_LOAD = 10**4

SUCCESS_FILENAME = os.getenv("SUCCESS_FILENAME", "_SUCCESS")

DEFAULT_JOB_PREFIX = "gcf-ingest-"

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
