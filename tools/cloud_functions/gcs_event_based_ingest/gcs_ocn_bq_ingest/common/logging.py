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
"""Common helper logging functions"""
import json
from typing import Optional, Union

from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.cloud.exceptions import ClientError


def log_bigquery_job(job: Union[bigquery.LoadJob, bigquery.QueryJob],
                     table: bigquery.TableReference,
                     message: Optional[str] = None,
                     severity: Optional[str] = 'NOTICE'):
    """
    Creates a structured log which includes a BigQuery job and table reference
    :param job:
    :param table:
    :param message:
    :param severity:
    """
    if job.error_result:
        severity = "ERROR"
        message = message or "BigQuery Job had errors."
    elif severity == "ERROR":
        message = message or ("BigQuery Job completed"
                              " but is considered an error.")
    else:
        severity = "NOTICE"
        message = message or "BigQuery Job completed without errors."

    print(
        json.dumps(
            dict(
                message=message,
                severity=severity,
                job=job.to_api_repr(),
                table=table.to_api_repr(),
                errors=job.error_result,
            )))


def log_with_table(
    table: bigquery.TableReference,
    message: str,
    severity: Optional[str] = 'NOTICE',
):
    """
    Creates a structured log which includes a BigQuery table reference
    :param table:
    :param message:
    :param severity:
    :return:
    """
    table_json = None
    if table is not None:
        table_json = table.to_api_repr()
    print(
        json.dumps(dict(
            message=message,
            severity=severity,
            table=table_json,
        )))


def log_api_error(table: bigquery.TableReference, message: str,
                  error: Union[GoogleAPIError, ClientError]):
    """
    Creates a structured api error log which includes a BigQuery table reference
    :param table:
    :param message:
    :param error:
    :return:
    """
    print(
        json.dumps(
            dict(message=message or error.message,
                 severity='ERROR',
                 table=table.to_api_repr(),
                 errors=error.errors or error.message or message)))
