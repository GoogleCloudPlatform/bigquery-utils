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

""" 11) Script to query Bigquery, with added support for logging query and script statistics """

from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.logging.handlers import CloudLoggingHandler

import google.cloud.logging
import os
import os.path
import sys
import logging
import json
import traceback
import ast

__author__ = 'nikunjbhartia@google.com (Nikunj Bhartia)'

StartTime = datetime.now()

# Reference : https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

_LOGGER_NAME = "bqclient"
_LOG_LEVEL = logging.DEBUG
_LOG_FILE = "logs/" + datetime.now().strftime("%Y%m%d") + "_" + "bqclient.log"

# This file has to be created based on the documentation :
# https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-console
_CRED_FILE = "credentials/service_account.json"

# When your application needs access to user data, it asks Google for a particular scope of access.
# Access tokens are associated with a scope, which limits the token's access
# References: https://developers.google.com/drive/api/v2/about-auth
#             https://cloud.google.com/bigquery/docs/authorization
_CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

_CREDENTIALS = service_account.Credentials.from_service_account_file(
    _CRED_FILE,
    scopes=[_CLOUD_PLATFORM_SCOPE],
)

_PROJECT_ID = _CREDENTIALS.project_id

_QUERY = None

# For named params
_QUERY_FILE = "resources/query_named_params.sql"
_QUERY_PARAMS = "[('tags','STRING','%google-bigquery%'), ('limit','INT64',10)]"

# For positional params
# _QUERY_FILE = "resources/query_positional_params.sql"
# _QUERY_PARAMS = "[(None,'STRING','%google-bigquery%'), (None,'INT64',10)]"

_DRY_RUN_FLAG = False
_QUERY_CACHE_FLAG = True

Logger = None
BigqueryClient = None


class CustomJsonFormatter(logging.Formatter):
    """Custom Log formatter for supporting structured logging.

    Args:
        message : Log text
        extraAgrs : [Optional] A dictionary of extra parameters to be clubbed with the final json log structure

    Returns:
        Json log structure (dict of k,v pairs) clubbed with input arguments and extra log record attributes
    """
    def format(self, record):
        # We need this because If we simply log a struct, stackdriver would still take it as a string
        # Reference : https://googleapis.dev/python/logging/latest/handlers.html
        # Note : A logRecord instance is created automatically whenever a logger logs something
        # For more info about log record attributes : https://docs.python.org/3/library/logging.html#logrecord-attributes
        log_struct = {} if record.args == () else record.args
        log_struct['logText'] = record.msg
        log_struct['filename'] = record.filename
        log_struct['funcName'] = record.funcName
        log_struct['processId'] = record.process

        # Can be used as an identifier while debugging
        log_struct['threadId'] = record.thread
        log_struct['levelName'] = record.levelname
        log_struct['eventTimestamp'] = datetime.fromtimestamp(record.created).strftime(_DATE_FORMAT)

        return  json.dumps(log_struct)

def log_params():
    """Logs all the script global flags and parameters. Valid only in case debug mode is on. """
    globalparams = {
        "credFilepath" : _CRED_FILE,
        "projectId" : _PROJECT_ID,
        "dryRun" : _DRY_RUN_FLAG,
        "queryCacheFlag" : _QUERY_CACHE_FLAG,
        "queryParams" : _QUERY_PARAMS,
        "query" : _QUERY,
        "queryFile" : _QUERY_FILE,
        "logFile" : _LOG_FILE,
        "loggerName" : _LOGGER_NAME,
        "logLevel" : logging.getLevelName(_LOG_LEVEL),
        "serviceAccount" : _CREDENTIALS.service_account_email
    }

    Logger.debug("Script Properties", globalparams)


def create_directories(filename):
    """Creates directory paths for input log file in case given path doesn't exist"""
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename), exist_ok=True)


def init_logger():
    """Initializing the logger global variable for stackdriver logging"""
    global Logger

    create_directories(_LOG_FILE)

    Logger = logging.getLogger(_LOGGER_NAME)

    cloudlog_client = google.cloud.logging.Client(credentials=_CREDENTIALS, project=_PROJECT_ID)
    cloudlog_handler = CloudLoggingHandler(cloudlog_client)
    cloudlog_handler.setFormatter(CustomJsonFormatter())

    filelog_handler = logging.handlers.RotatingFileHandler(_LOG_FILE, maxBytes=1024*100, backupCount=5)
    filelog_handler.setFormatter(CustomJsonFormatter())

    # Add log handlers for both stakdriver and file to logger
    Logger.addHandler(cloudlog_handler)
    Logger.addHandler(filelog_handler)

    Logger.setLevel(_LOG_LEVEL)


def init_bigquery_client():
    """Initializes bigquery client along with query flags and query paramaters"""
    global BigqueryClient

    # Reference for job parameters : https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJobConfig.html
    bq_jobconfig = bigquery.QueryJobConfig()

    # Support for parameterized queries
    # Reference :https://cloud.google.com/bigquery/docs/parameterized-queries
    if _QUERY_PARAMS:
        params = []
        # ast.literal_eval(x) is used to convert string representation of a list to list object
        for elem in ast.literal_eval(_QUERY_PARAMS):
            # consider input parameter only if all the three key values : ("variable-name", "variable-datatype", "variable-value") are given
            if len(elem) == 3:
                params.append(bigquery.ScalarQueryParameter(elem[0], elem[1], elem[2]))
            else:
                Logger.info("Skipping an input parameter. One of the three values ('var1-name' or None, 'var1-datatype', 'var1-value') missing in : %s"%(str(elem)))

        bq_jobconfig.query_parameters = params

    bq_jobconfig.dry_run = _DRY_RUN_FLAG
    bq_jobconfig.use_query_cache = _QUERY_CACHE_FLAG

    BigqueryClient = bigquery.Client(credentials=_CREDENTIALS, project=_PROJECT_ID, default_query_job_config=bq_jobconfig)


def init_query_text():
    """Initializing the query string"""
    global _QUERY
    try:
        # If query file is not passed as input argument, then it takes the input query text (set during the initializing global params )
        if _QUERY_FILE:
            with open(_QUERY_FILE, 'r') as file:
                _QUERY = file.read()
    except Exception as error:
        Logger.error("Exception during query fetch", { 'exceptionInfo' : traceback.format_exc() })
        raise

    Logger.info("Query fetch successful", { 'query' : _QUERY })


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

        # dry run mode doesn't have a result
        if not _DRY_RUN_FLAG:
            # The below function implements a blocking call until query completion or timeout
            process_query_results(query_job)

        log_query_stats(query_job)

    except KeyError as error:
        Logger.error("Failure in fetching query stats ", { 'exceptionInfo' : traceback.format_exc() })
        raise
    except AttributeError as error:
        Logger.error("Failure in fetching query stats ", { 'exceptionInfo' : traceback.format_exc() })
        raise
    except Exception as error:
        Logger.error("Exception during query execution ", { 'exceptionInfo' : traceback.format_exc() })
        raise

    return exit_code


def process_query_results(query_job):
    """ Modify to process the results as per requirement. Below example prints every row"""

     # This is Blocking Call and bails out until timeout is exceeded or query returns successfully
    results = query_job.result()
    for row in results:
        # row._class_ = google.cloud.bigquery.table.Row
        # Eg: row => Row(('https://stackoverflow.com/questions/22879669', 48540), {'url': 0, 'view_count': 1})
        #     list(row.items()) =>  [('url', 'https://stackoverflow.com/questions/22879669'),('view_count', 48540)]
        rowdict = dict(list(row.items()))
        Logger.info("Query result for every row", { 'result' : rowdict, 'jobId' : query_job.job_id })


def log_query_stats(query_job):
    """Logs query stats. Valid when debug mode is on"""
    query_stats = { 'state' : query_job.state,
                    'bytesProcessed' : query_job.total_bytes_processed,
                    'cacheHit' : query_job.cache_hit,
                    'jobId' : query_job.job_id }

    # Calculating Extra query stats in case dry run flag is not set as true
    if not _DRY_RUN_FLAG:
        queryStartTime =  datetime.fromtimestamp(query_job._properties['statistics']['startTime']/1000)
        queryEndTime = datetime.fromtimestamp(query_job._properties['statistics']['endTime']/1000)

        query_stats['queryStartTime'] = queryStartTime.strftime(_DATE_FORMAT)
        query_stats['queryEndTime'] = queryEndTime.strftime(_DATE_FORMAT)
        query_stats['queryRuntimeSecs'] = (queryEndTime - queryStartTime).total_seconds()

    Logger.debug("Query Statistics", { 'queryStats' :  query_stats })


if __name__ == '__main__':
    init_logger()
    init_bigquery_client()
    init_query_text()
    log_params()
    exit_code = run_query()

    EndTime = datetime.now()

    Logger.debug("Script statistics", { 'scriptStartTime' : StartTime.strftime(_DATE_FORMAT),
                                        'scriptEndTime' : EndTime.strftime(_DATE_FORMAT),
                                        'scriptRuntimeSecs' : (EndTime - StartTime).total_seconds() })

    sys.exit(exit_code)
