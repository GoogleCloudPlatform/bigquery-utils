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
"""Custom Exceptions of GCS event based ingest to BigQuery"""


class OneLineException(Exception):
    """base class for exceptions whose messages will be displayed on a single
    line for better readability in Cloud Function Logs"""

    def __init__(self, msg):
        super().__init__(msg.replace('\n', ' ').replace('\r', ''))


class DuplicateNotificationException(OneLineException):
    """Exception to indicate that the function was triggered twice for the same
    event."""


class BigQueryJobFailure(OneLineException):
    """Exception to indicate that there was an issue with a BigQuery job. This
    might include client errors (e.g. bad request which can happen if a _SUCCESS
    file is dropped but there are not data files at the GCS prefix) or server
    side errors like a job that fails to execute successfully."""


class DestinationRegexMatchException(OneLineException):
    """Exception to indicate that a success file did not match the destination
    regex specified in the DESTINATION_REGEX environment variable (or the
    default)"""


class HiveSourceUriPrefixRegexMatchException(OneLineException):
    """Exception to indicate that a hive source uri prefix could not be found."""


class UnexpectedTriggerException(OneLineException):
    """Exception to indicate the cloud function was triggered with an unexpected
    payload."""


class BacklogException(OneLineException):
    """Exception to indicate an issue with the backlog mechanics of this
    function."""


EXCEPTIONS_TO_REPORT = (
    BigQueryJobFailure,
    UnexpectedTriggerException,
    DestinationRegexMatchException,
    BacklogException,
)
