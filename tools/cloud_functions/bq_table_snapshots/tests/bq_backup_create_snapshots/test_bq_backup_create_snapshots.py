# Copyright 2023 Google LLC
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
"""unit tests for bq_backup_create_snapshots"""

from requests import request
from bq_backup_create_snapshots.main import get_snapshot_timestamp
import pytest
import time
from google.cloud import bigquery


@pytest.mark.parametrize(
    "crontab_format",
    [
        ("10 * * * *"),
        ("30 * * * *"),
        ("0 1 * * *")
    ])
def test_filter_tables(crontab_format):
    """ensures that snapshots messages recieved within seconds of 
    eachother result in snapshots representing the same point in 
    time
    """
    message = {"crontab_format":crontab_format}
    timestamps = []
    for i in range(3):
        timestamps.append(get_snapshot_timestamp)
        time.sleep(1)
    assert len(set(timestamps)) == 1
