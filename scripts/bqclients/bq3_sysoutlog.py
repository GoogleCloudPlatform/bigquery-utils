#!/usr/bin/python3.7
#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

""" 3)Script to query Bigquery, with improved logging support using default python formatter with custom modifiers in stdout """

from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import traceback
import sys

__author__ = 'nikunjbhartia@google.com (Nikunj Bhartia)'

_QUERY = None
Logger = None
BigqueryClient = None

def init_logger():
    """Initializing the logger global variable for stdout"""
    global Logger

    logger_name = "bqclient"
    log_level = logging.DEBUG
    # Using unstructured string logging formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    #Defining handler for std.out
    stdoutlog_handler = logging.StreamHandler(sys.stdout)
    stdoutlog_handler.setFormatter(formatter)

    Logger = logging.getLogger(logger_name)
    Logger.addHandler(stdoutlog_handler)
    Logger.setLevel(log_level)


def init_bigquery_client():
    """Iniitializing Bigquery Client using env variable for authentication"""
    global BigqueryClient

    credfile = "credentials/service_account.json"
    cloud_platform_scope = "https://www.googleapis.com/auth/cloud-platform"

    # When your application needs access to user data, it asks Google for a particular scope of access.
    # Access tokens are associated with a scope, which limits the token's access
    # References: https://developers.google.com/drive/api/v2/about-auth
    #             https://cloud.google.com/bigquery/docs/authorization
    #             https://tools.ietf.org/html/rfc6749#section-3.3
    credentials = service_account.Credentials.from_service_account_file(
        credfile,
        scopes=[cloud_platform_scope],
    )

    #Takes credential file path from the env varibale : GOOGLE_APPLICATION_CREDENTIALS
    #More details Google's Application Default Credentials Strategy(ADC) strategy : https://cloud.google.com/docs/authentication/production
    BigqueryClient = bigquery.Client(credentials=credentials, project=credentials.project_id)


def init_query_text():
    """Initializing the query string"""
    global _QUERY

    _QUERY = """ SELECT
                  CONCAT(
                    'https://stackoverflow.com/questions/',
                    CAST(id as STRING)) as url,
                  view_count
                FROM `bigquery-public-data.stackoverflow.posts_questions`
                WHERE tags like '%google-bigquery%'
                ORDER BY view_count DESC
                LIMIT 10"""

    Logger.info("Input Query String : %s"%(_QUERY))

def run_query():
    """Runs a BigQuery SQL query in synchronous mode and print results if the query completes within a specified timeout.

    Returns:
        0 in case of success, nonzero on failure.

    Raises:
        Exception : Failure during query runtime execution
    """
    # Setting default exit code
    exit_code = 0
    try :
        Logger.debug("Executing query")
        # For synchronous call
        query_job = BigqueryClient.query(_QUERY)  #API request
        process_query_results(query_job)

    except Exception as error:
        Logger.error("Exception during query execution %s" %(traceback.format_exc()))
        raise

    return exit_code


def process_query_results(query_job):
    """ Modify to process the results as per requirement. Below example prints every row"""

     # This is Blocking Call and bails out until timeout is exceeded or query returns successfully
    results = query_job.result()
    for row in results:
        # row._class_ = google.cloud.bigquery.table.Row
        # Eg: row => Row(('https://stackoverflow.com/questions/22879669', 48540), {'url': 0, 'view_count': 1})
        Logger.info("Url : %s, Views : %s" %(row.url, row.view_count))


if __name__ == '__main__':
    init_logger()
    init_bigquery_client()
    init_query_text()
    exit_code = run_query()
    sys.exit(exit_code)
