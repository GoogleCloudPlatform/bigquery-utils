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
"""Custom Exceptions of GCS event based ingest to BigQuery"""


class DuplicateNotificationException(Exception):
    """Exception to indicate that the function was triggered twice for the same
    event."""


class BigQueryJobFailure(Exception):
    """Exception to indicate that the function was triggered twice for the same
    event."""


class DestinationRegexMatchException(Exception):
    """Exception to indicate that a success file did not match the destination
    regex specified in the DESTINATION_REGEX environment variable (or the
    default)"""


class UnexpectedTriggerException(Exception):
    """Exception to indicate the cloud function was triggered with an unexpected
    payload."""


class BacklogException(Exception):
    """Exception to indicate an issue with the backlog mechanics of this
    function."""


EXCEPTIONS_TO_REPORT = (
    BigQueryJobFailure,
    UnexpectedTriggerException,
    DestinationRegexMatchException,
    BacklogException,
)
