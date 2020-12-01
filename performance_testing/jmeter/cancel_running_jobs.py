# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from argparse import ArgumentParser
from google.cloud import bigquery


def cancel_jobs(client):
    for job in client.list_jobs(all_users=True, state_filter="RUNNING"):
        client.cancel_job(job.job_id, location='us')


def get_cmd_line_args():
    parser = ArgumentParser()
    parser.add_argument(
        '--project_id',
        help='Project in which all running BigQuery jobs will be cancelled.')
    return parser.parse_args()


def main():
    args = get_cmd_line_args()
    cancel_jobs(bigquery.Client(project=args.project_id))


if __name__ == '__main__':
    main()
