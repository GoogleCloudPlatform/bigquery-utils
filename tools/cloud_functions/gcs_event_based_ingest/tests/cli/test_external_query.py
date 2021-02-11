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
"""integrtion tests for gcs_ocn_bq_ingest"""
import json
import os

import external_query
import pytest

TEST_DIR = os.path.realpath(os.path.dirname(__file__) + "/..")


@pytest.mark.IT
@pytest.mark.CLI
def test_dry_run_external(tmp_path):
    """
    Test basic functionality of dry running and external query.
    """
    query_path = tmp_path / "test.sql"
    query_path.write_text("SELECT * FROM temp_ext")

    with open(os.path.join(TEST_DIR, "resources",
                           "nation_schema.json")) as schema:
        fields = json.load(schema)
    config = {
        "schema": {
            "fields": fields
        },
        "csvOptions": {
            "allowJaggedRows": False,
            "allowQuotedNewlines": False,
            "encoding": "UTF-8",
            "fieldDelimiter": "|",
            "skipLeadingRows": 0,
        },
        "sourceFormat": "CSV",
    }
    external_path = tmp_path / "external.json"
    external_path.write_text(json.dumps(config))

    args = external_query.parse_args(
        [f"-q={query_path}", f"-e={external_path}", "--dry-run"])
    external_query.main(args)


@pytest.mark.IT
@pytest.mark.CLI
def test_failed_dry_run_external(tmp_path):
    """
    Test failed dry run.
    """
    query_path = tmp_path / "test.sql"
    # foo is not in the nation_schema
    query_path.write_text("SELECT foo FROM temp_ext")

    with open(os.path.join(TEST_DIR, "resources",
                           "nation_schema.json")) as schema:
        fields = json.load(schema)
    config = {
        "schema": {
            "fields": fields
        },
        "csvOptions": {
            "allowJaggedRows": False,
            "allowQuotedNewlines": False,
            "encoding": "UTF-8",
            "fieldDelimiter": "|",
            "skipLeadingRows": 0,
        },
        "sourceFormat": "CSV",
    }
    external_path = tmp_path / "external.json"
    external_path.write_text(json.dumps(config))

    args = external_query.parse_args(
        [f"-q={query_path}", f"-e={external_path}", "--dry-run"])
    raised = False
    try:
        external_query.main(args)
    except Exception:
        raised = True
    assert raised
