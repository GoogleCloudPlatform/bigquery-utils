# Copyright 2025 Google LLC
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

r"""This script facilitates the migration of BigQuery Policy Tags and associated Data Policies between regions, designed to assist with scenarios like replicating a dataset and ensuring governance consistency.

*** GOOGLE CONFIDENTIAL - PROVIDED FOR CVS HEALTH USE ONLY ***
*** AS IS, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED ***
*** NOT AN OFFICIALLY SUPPORTED GOOGLE PRODUCT ***

**Understanding the Problem:**

When a BigQuery dataset is replicated across regions (e.g., from 'us' to 'us-east4'),
the table schemas are replicated, but the Data Catalog Policy Tags attached to the
columns are not fully functional in the destination region. The policy tag IDs
are retained, but they point to a non-existent taxonomy (ID 0) in the new region.
This causes queries on tagged columns in the secondary region to fail with
"Access Denied" errors, as the fine-grained access control cannot be resolved.

**How This Script Helps:**

This script automates the process of recreating the governance artifacts
(Taxonomies, Policy Tag IAM Policies, Data Policies) in the destination region
and re-linking the table columns to the correct, newly replicated policy tags.
This script is provided to assist CVS Health with specific migration
challenges. This script is designed to operate on one dataset at a time to
minimize risk during migration.

**Testing:**

It is HIGHLY RECOMMENDED to test this script on a non-production dataset and
verify the migration of policy tags is correct before using the script on a
production dataset.

**The Migration Steps:**

1.  **Save Policy Tag Bindings (Step 1):** Before promotion, this step snapshots the
    current state of which policy tags are bound to which table columns in the
    source region for the specified dataset(s). This is crucial for reference and rollback.
    Required args: --project_id, --source_region, one of --dataset or --all_datasets, and --table_schema_backup_dir
2.  **Replicate Taxonomy Tree (Step 2):** Copies the entire taxonomy structure
    (the hierarchy of policy tags) from the source to the destination region.
    The taxonomies in the destination region will have the region name appended to their display names.
    Required args: --project_id, --source_region, --destination_region, --policy_tag_bindings_snapshot_timestamp
3.  **Replicate Policy Tag IAM Policies (Step 3):** Copies the IAM permissions
    (e.g., who has FineGrainedReader) from each policy tag in the source
    region to the corresponding policy tag in the destination region.
    Required args: --project_id, --source_region, --destination_region, --policy_tag_bindings_snapshot_timestamp
4.  **Replicate Data Policies (Step 4):** Copies the data masking rules
    (Data Policies) associated with the policy tags from the source to the
    destination region, linking them to the newly replicated policy tags.
    This also includes replicating the IAM permissions on the Data Policies.
    If a data policy uses a custom masking routine, this step will attempt to
    replicate the routine to the dataset specified by --destination_custom_masking_routine_dataset
    in the destination region.
    Required args: --project_id, --source_region, --destination_region, --policy_tag_bindings_snapshot_timestamp, --destination_custom_masking_routine_dataset
5.  **Promote Replica to Primary (Step 5):** Swaps the primary and secondary
    roles of the dataset replicas. The destination region becomes writable.
    *** CRITICAL WARNING ***: After this step, columns with policy tags
    WILL BECOME INACCESSIBLE in BOTH the source region ('us')
    and the destination region ('us-east4') until Step 6 is run.
    This is because the policy tag linkages need to be updated in the new
    primary.
    It is HIGHLY RECOMMENDED to run Step 6 immediately after this step.
    **IMPORTANT**: Step 5 includes a 2-minute pause after completion to allow
    the primary replica switch to fully propagate before Step 6 can be run.
    Required args: --project_id, --source_region, --destination_region, --dataset
6.  **Update Table Schemas (Step 6):** After the promotion, this step updates
    the table schemas in the NEW primary (destination region). It corrects the
    policy tag references on each column to point to the now-valid taxonomy IDs
    in the destination region, restoring access.
    Required args: --project_id, --source_region, --destination_region, one of --dataset or --all_datasets, --policy_tag_bindings_snapshot_timestamp

**Prerequisites:**
1.  Python 3.9+ installed.
2.  Google Cloud SDK (gcloud) installed and authenticated:
    - Run 'gcloud auth login'
    - Run 'gcloud auth application-default login'
3.  Required Python libraries installed (use a virtual environment):
    - python3 -m venv .venv
    - source .venv/bin/activate
    - pip install google-cloud-bigquery google-cloud-datacatalog google-cloud-bigquery-datapolicies google-api-python-client
4.  The user or service account running this script needs sufficient IAM permissions
    in the project, including but not limited to:
    - BigQuery Admin (roles/bigquery.admin)
    - Data Catalog Admin (roles/datacatalog.admin)
    - Data Catalog Policy Tag Admin (roles/datacatalog.categoryAdmin)
    - BigQuery Data Policy Admin (roles/bigquerydatapolicy.admin)
    - BigQuery Data Editor (roles/bigquery.dataEditor) for the datasets containing routines.
    - Service Usage Consumer (roles/serviceusage.serviceUsageConsumer)
    - IAM permissions to get/set IAM policies on policy tags and data policies.

**IAM Permissions per Step:**

While the script can be run by a user with broad permissions (like BigQuery
Admin, Data Catalog Admin, and Data Policy Admin), if different users or
service accounts are responsible for different steps, the following minimum
permissions are required for each step:

*   **Step 1:**
    *   `bigquery.jobs.create`
    *   `bigquery.tables.list` on source dataset (for `INFORMATION_SCHEMA`).
    *   `bigquery.datasets.create`, `bigquery.datasets.get`, `bigquery.tables.create`
        (for policy tag binding snapshot dataset/table).
*   **Step 2:**
    *   `bigquery.jobs.create`, `bigquery.tables.getData` (to read snapshot table).
    *   `datacatalog.taxonomies.get`, `datacatalog.policyTags.get` (to export).
    *   `datacatalog.taxonomies.import`, `datacatalog.taxonomies.create` (to import).
*   **Step 3:**
    *   `bigquery.jobs.create`, `bigquery.tables.getData` (to read snapshot table).
    *   `datacatalog.taxonomies.list`, `datacatalog.taxonomies.get`,
        `datacatalog.policyTags.list` (to read taxonomies/tags in both regions).
    *   `datacatalog.policyTags.getIamPolicy`, `datacatalog.policyTags.setIamPolicy`
        (to replicate IAM policies).
*   **Step 4:**
    *   `bigquery.jobs.create`, `bigquery.tables.getData` (to read snapshot table).
    *   `datacatalog.taxonomies.list`, `datacatalog.taxonomies.get`,
        `datacatalog.policyTags.list` (to read taxonomies/tags in both regions).
    *   `bigquery.dataPolicies.list` (in source region).
    *   `bigquery.dataPolicies.create` (in destination region).
    *   `bigquery.dataPolicies.getIamPolicy`, `bigquery.dataPolicies.setIamPolicy`
        (to replicate IAM policies).
    *   If custom routines are used: `bigquery.routines.get` (source),
        `bigquery.datasets.create`, `bigquery.datasets.get`,
        `bigquery.routines.create` (destination).
*   **Step 5:**
    *   `bigquery.datasets.update` on dataset being promoted.
*   **Step 6:**
    *   `bigquery.jobs.create`, `bigquery.tables.getData` (to read snapshot table).
    *   `datacatalog.taxonomies.list`, `datacatalog.taxonomies.get`,
        `datacatalog.policyTags.list` (to read taxonomies/tags in both regions).
    *   `bigquery.tables.get`, `bigquery.tables.update` (on tables in destination
        region).
    *   Read/write access to the `--table_schema_backup_dir` if provided.

**Idempotency:**

The script is designed to be run step-by-step, but it's useful to know
what happens if a step is rerun:

*   **Step 1:** Not idempotent. Rerunning Step 1 with the same
    `--policy_tag_bindings_snapshot_timestamp` will fail if the snapshot table
    from the first run already exists. Each run of Step 1 should ideally
    create a new snapshot with a new timestamp.
*   **Step 2:** Not idempotent. This step creates taxonomies in the destination
    region with a region suffix in their display name. If you rerun this step
    after a successful run, it will fail with an "Already Exists" error
    because it will attempt to create taxonomies that already exist.
*   **Step 3:** Idempotent. This step replicates IAM policies from source policy
    tags to destination policy tags. Rerunning it will overwrite the IAM
    policies on the destination tags with the current policies from the source
    tags.
*   **Step 4:** Idempotent. This step creates data policies and routines in the
    destination region. If a data policy or routine already exists, it skips
    creation and proceeds to replicate its IAM policy. Rerunning it will create
    any missing data policies/routines and ensure IAM policies are synced from
    source to destination.
*   **Step 5:** Idempotent. This step promotes a dataset replica to primary.
    Rerunning it on a dataset that has already been promoted has no effect.
*   **Step 6:** Idempotent. This step updates table schemas to point to policy
    tags in the destination region. It only performs an update if a column's
    policy tag reference points to a non-existent taxonomy (`taxonomies/0`).
    If tags have already been updated by a previous run, rerunning this step
    will detect no changes are needed and skip the update.

Usage:
  source .venv/bin/activate
  python migrate_policy_tags.py --project_id <YOUR_PROJECT_ID> [options]

Examples:

  # Run Step 1 for a single dataset
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --dataset cvs_test_us --step1 \
    --table_schema_backup_dir ./schema_backups

  # Run Step 1 for all datasets
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --all_datasets --step1 \
    --table_schema_backup_dir ./schema_backups

  # Run Step 2 only
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --destination_region us-east4 --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> --step2

  # Run Step 3 only
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --destination_region us-east4 --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> --step3

  # Run Step 4 only
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --destination_region us-east4 --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> \
    --destination_custom_masking_routine_dataset my_routines_us_east4 --step4

  # Run Step 5 only
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --destination_region us-east4 --dataset cvs_test_us --step5

  # Run Step 6 only
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --destination_region us-east4 --dataset cvs_test_us \
    --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> --step6

  # Run Step 6 for all datasets in snapshot
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --destination_region us-east4 --all_datasets \
    --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> --step6

  # Log to a specific file and set log level to DEBUG
  python migrate_policy_tags.py --project_id policy-tags-migration-test \
    --source_region us --dataset cvs_test_us --step1 --log_file ./migration_run.log --log_level DEBUG
"""

# This is a customer-facing script provided as-is, and is not subject to
# internal linting. Please keep pylint disabled.
# pylint:disable=all

import argparse
from datetime import datetime
import logging
import os
import sys
from typing import List, Optional, Dict, Tuple
import time
import re
import json
import difflib
from googleapiclient import discovery

from google.api_core import exceptions as api_core_exceptions
from google.api_core import retry
from google.cloud import bigquery
from google.cloud import datacatalog_v1
from google.cloud import bigquery_datapolicies_v1
from google.cloud import exceptions as cloud_exceptions
from google.iam.v1 import iam_policy_pb2  # Import IAM policy protos
from google.iam.v1 import policy_pb2

# --- Configuration for BigQuery table to store policy tag binding snapshots ---
DEFAULT_BINDINGS_DATASET = "policy_tag_bindings_dataset"
BINDINGS_TABLE_PREFIX = "policy_tag_bindings_"

# Define a default retry configuration for API calls
# This will retry on common transient errors like 503, 429, etc.
DEFAULT_RETRY = retry.Retry(
    initial=1.0,  # Initial delay in seconds
    maximum=60.0,  # Maximum delay in seconds
    multiplier=2.0, # Backoff factor
    deadline=300.0, # Total time in seconds to keep retrying
)

# --- Logger Setup ---
logger = logging.getLogger(__name__)


def _get_project_id_or_number(resource_name: str) -> str:
    """Extracts project ID or number from a resource name."""
    match = re.match(r"projects/([^/]+)/.*", resource_name)
    if match:
        return match.group(1)
    raise ValueError(f"Could not parse project from resource name: {resource_name}")

def _group_by_project(resource_names: List[str]) -> Dict[str, List[str]]:
    """Groups resource names by project."""
    grouped = {}
    for name in resource_names:
        try:
            project = _get_project_id_or_number(name)
            if project not in grouped:
                grouped[project] = []
            grouped[project].append(name)
        except ValueError as e:
            logger.warning("Skipping resource name %s: %s", name, e)
    return grouped

def _setup_logging(log_file_path: Optional[str] = None, log_level: int = logging.INFO) -> None:
    """Configures logging to console and optionally to a file."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger.setLevel(log_level)
    # Prevent log messages from propagating to the root logger,
    # ensuring they are only handled by the handlers defined here.
    logger.propagate = False

    # Clear existing handlers.
    # This prevents duplicate log messages if _setup_logging is called multiple times.
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    # File Handler
    if log_file_path:
        log_dir = os.path.dirname(log_file_path)
        # Create log directory if it doesn't exist.
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
                print(f"Created log directory: {log_dir}")
            except Exception as e:
                print(f"Warning: Could not create log directory {log_dir}: {e}")
        try:
            file_handler = logging.FileHandler(log_file_path, mode='a')
            file_handler.setFormatter(log_formatter)
            logger.addHandler(file_handler)
            logging.info(f"Logging to file: {os.path.abspath(log_file_path)}")
        except Exception as e:
            logging.error(f"Failed to set up file logging to {log_file_path}: {e}")
            print(f"Failed to set up file logging to {log_file_path}: {e}")
            print("Logging to console only.")
    else:
        logging.info("Logging to console only.")

def _create_bindings_dataset_if_not_exists(
    bq_client: bigquery.Client,
    project_qualified_bindings_dataset: str,
    location: str,
) -> None:
    """Ensures the dataset for storing policy tag binding snapshots exists.

    Args:
        bq_client: The BigQuery client.
        project_qualified_bindings_dataset: The project-qualified dataset ID
            for policy tag bindings snapshot table.
        location: The region or multi-region for the dataset, e.g., "us" or "us-east4".
    """
    dataset_id = project_qualified_bindings_dataset

    try:
        # Check if dataset already exists.
        bq_client.get_dataset(dataset_id, retry=DEFAULT_RETRY)
        logger.info("Policy tag bindings dataset '%s' already exists.", dataset_id)
    except cloud_exceptions.NotFound:
        # If dataset doesn't exist, create it.
        logger.info(
            "Policy tag bindings dataset '%s' does not exist in location '%s', creating it ...",
            dataset_id,
            location,
        )
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = location
        bq_client.create_dataset(dataset, timeout=30, retry=DEFAULT_RETRY)
        logger.info("Policy tag bindings dataset '%s' created.", dataset_id)


def save_policy_tag_bindings(
    bq_client: bigquery.Client,
    project_id: str,
    source_region: str,
    dataset: Optional[str],
    all_datasets: bool,
    timestamp_str: str,
    project_qualified_bindings_dataset: str,
    create_bindings_dataset_if_not_exists: bool,
    table_schema_backup_dir: Optional[str] = None,
) -> None:
    """Step 1: Save table column to policy tag bindings.

    This function queries the INFORMATION_SCHEMA.COLUMN_FIELD_PATHS in the
    source region to capture the current policy tag bindings for all columns
    within the specified dataset or all datasets. The snapshot is saved to a
    BigQuery table in a dedicated dataset (`policy_tag_bindings_dataset`)
    within the source region.

    Args:
        bq_client: The BigQuery client.
        project_id: The Google Cloud project ID.
        source_region: The source region of the BigQuery dataset (e.g., "us").
        dataset: The ID of the dataset to snapshot policy tag bindings from.
        all_datasets: If True, snapshot policy tag bindings for all datasets.
        timestamp_str: A timestamp string in YYYYMMDDHHMMSS format, used to
            create a unique snapshot table name.
        project_qualified_bindings_dataset: Project-qualified dataset for
            bindings table.
        create_bindings_dataset_if_not_exists: Whether to create bindings
            dataset if it doesn't exist.
        table_schema_backup_dir: Optional. If provided, saves table schemas
            to this directory.

    Raises:
        ValueError: If `source_region` or `dataset` is not provided.
        Exception: If an error occurs during BigQuery query execution.
    """
    logger.info("--- [Started] Step 1: Saving Policy Tag Bindings ---")
    if not source_region:
        raise ValueError("--source_region is required for Step 1")
    if not dataset and not all_datasets:
        raise ValueError("Either --dataset or --all_datasets must be specified for Step 1")
    if dataset and all_datasets:
        raise ValueError("--dataset and --all_datasets cannot be specified together for Step 1")
    if not table_schema_backup_dir:
        raise ValueError("--table_schema_backup_dir is required for Step 1.")

    if create_bindings_dataset_if_not_exists:
        _create_bindings_dataset_if_not_exists(
            bq_client, project_qualified_bindings_dataset, source_region
        )

    snapshot_table_id = f"{BINDINGS_TABLE_PREFIX}{timestamp_str}"
    full_snapshot_table_id = (
        f"`{project_qualified_bindings_dataset}.{snapshot_table_id}`"
    )

    if all_datasets:
        description = f"Snapshot of column policy tag bindings for all datasets in project {project_id} from {source_region} taken at {timestamp_str}"
        where_clause = ""
    else:
        description = f"Snapshot of column policy tag bindings for dataset {dataset} from {source_region} taken at {timestamp_str}"
        where_clause = f"table_schema = '{dataset}' AND "

    sql = f"""
    CREATE TABLE {full_snapshot_table_id}
    OPTIONS (
        description = "{description}"
    ) AS
    SELECT
        '{project_id}' as native_project_id,
        table_catalog AS project_id,
        table_schema AS dataset_id,
        table_name,
        column_name,
        field_path,
        policy_tags,
        CURRENT_TIMESTAMP() as snapshot_timestamp
    FROM
      # Query COLUMN_FIELD_PATHS to get policy tags for all columns,
      # including nested fields in STRUCTs.
      `{project_id}`.`region-{source_region}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
    WHERE
      {where_clause}policy_tags IS NOT NULL AND ARRAY_LENGTH(policy_tags) > 0;
    """

    logger.info(
        "Executing query to save policy tag bindings to %s...",
        full_snapshot_table_id,
    )
    logger.info("SQL Statement:\n%s", sql)

    try:
        query_job = bq_client.query(sql, location=source_region)
        query_job.result()
        logger.info(
            "Successfully saved policy tag bindings to %s.",
            full_snapshot_table_id,
        )

        # Query the created table to log the number of rows.
        count_sql = f"SELECT COUNT(*) as row_count FROM {full_snapshot_table_id}"
        logger.info("Querying row count from %s...", full_snapshot_table_id)
        count_query_job = bq_client.query(count_sql, location=source_region)
        count_results = count_query_job.result()
        row_count = next(count_results).row_count
        logger.info("Snapshot table %s contains %d policy tag bindings.", full_snapshot_table_id, row_count)

        if table_schema_backup_dir:
            if not os.path.exists(table_schema_backup_dir):
                try:
                    logger.info("Creating backup directory: %s", table_schema_backup_dir)
                    os.makedirs(table_schema_backup_dir)
                    logger.info("Created backup directory: %s", table_schema_backup_dir)
                except OSError as e:
                    raise PermissionError(
                        f"Could not create backup directory {table_schema_backup_dir}: {e}"
                    ) from e
            elif not os.access(table_schema_backup_dir, os.W_OK):
                raise PermissionError(
                    f"Backup directory {table_schema_backup_dir} exists but is not writable."
                )
            logger.info("--- Backing up table schemas referenced in snapshot ---")
            tables_sql = (
                f"SELECT DISTINCT project_id, dataset_id, table_name "
                f"FROM {full_snapshot_table_id}"
            )
            logger.info("Querying tables for schema backup: %s", tables_sql)
            tables_query_job = bq_client.query(tables_sql, location=source_region)
            tables_results = tables_query_job.result()

            tables_to_backup = []
            for row in tables_results:
                tables_to_backup.append(
                    f"{row.project_id}.{row.dataset_id}.{row.table_name}"
                )

            logger.info(
                "Found %d tables in snapshot to back up schemas for.",
                len(tables_to_backup),
            )

            for table_ref_str in tables_to_backup:
                table = bq_client.get_table(table_ref_str, retry=DEFAULT_RETRY)
                _backup_schema(table, table_schema_backup_dir, timestamp_str)
            logger.info("--- Finished backing up table schemas ---")

    except Exception as e:
        logger.exception("Error saving policy tag bindings: %s", e)
        raise
    logger.info("--- [Finished] Step 1: Saving Policy Tag Bindings ---\n")


def _get_project_number(project_id: str) -> str | None:
    """Looks up project number from project ID using Cloud Resource Manager API."""
    try:
        crm_service = discovery.build('cloudresourcemanager', 'v1')
        request = crm_service.projects().get(projectId=project_id)
        response = request.execute()
        project_number = response.get('projectNumber')
        if project_number and project_number.isdigit():
            logger.info("Looked up project number for %s: %s", project_id, project_number)
            return project_number
        else:
            logger.error("Cloud Resource Manager API did not return a valid project number for %s.", project_id)
            return None
    except Exception as e:
        logger.exception("Failed to lookup project number for %s using Cloud Resource Manager API: %s", project_id, e)
        return None

def _list_all_taxonomies(
    ptm_client: datacatalog_v1.PolicyTagManagerClient, project_id: str, region: str
) -> List[str]:
    """Helper function to list all taxonomies in a given project and region.

    Ensures that returned taxonomy names use project number, not project ID.
    """
    # TODO: b/460103935 - Try use project_number here in the `parent`, in which
    # case to see whether the taxonomies in the list response use project number
    # in the names. If so, we don't need the later step that replaces project ID
    # with project number in the names.
    parent = f"projects/{project_id}/locations/{region}"
    logger.info("Listing all taxonomies in %s...", parent)
    try:
        taxonomies = ptm_client.list_taxonomies(
            request={'parent': parent},
            retry=DEFAULT_RETRY
        )
        raw_taxonomy_names = [taxonomy.name for taxonomy in taxonomies]
        project_number = _get_project_number(project_id)

        if not project_number:
            raise RuntimeError(f"Could not determine project number for project {project_id}. "
                               "This is required for consistent resource name handling. "
                               "Ensure gcloud is installed, authenticated, and has permission to describe the project.")

        taxonomy_names = [
            name.replace(f'projects/{project_id}/', f'projects/{project_number}/')
            for name in raw_taxonomy_names
        ]

        logger.info(
            "Found %d taxonomies in %s.",
            len(taxonomy_names),
            parent,
        )
        for name in taxonomy_names:
            logger.info("  - %s", name)
        return taxonomy_names
    except Exception as e:
        logger.exception("Error listing all taxonomies in %s: %s", parent, e)
        raise


def _get_taxonomies_from_snapshot_table(
    bq_client: bigquery.Client,
    project_id: str,
    source_region: str,
    timestamp_str: str,
    project_qualified_bindings_dataset: str,
) -> List[str]:
    """Helper function to get unique taxonomy names from the snapshot table.

    Args:
        bq_client: The BigQuery client.
        project_id: The Google Cloud project ID.
        source_region: The source region of the BigQuery dataset.
        timestamp_str: A timestamp string in YYYYMMDDHHMMSS format, used to
            identify the snapshot table (e.g., "20250115143000").
        project_qualified_bindings_dataset: Project-qualified dataset for
            bindings table.

    Returns:
        A list of unique taxonomy resource names found in the snapshot.
    """
    snapshot_table_id = f"{BINDINGS_TABLE_PREFIX}{timestamp_str}"
    full_snapshot_table_id = (
        f"`{project_qualified_bindings_dataset}.{snapshot_table_id}`"
    )

    # SQL to extract taxonomy name from the full policy tag resource name string.
    # Policy tag names are in the format:
    # projects/{p}/locations/{l}/taxonomies/{t}/policyTags/{pt}
    # This query extracts the 'projects/{p}/locations/{l}/taxonomies/{t}' part.
    sql = f"""
    SELECT DISTINCT
        SUBSTR(policy_tag_name, 0, INSTR(policy_tag_name, '/policyTags/') - 1) as taxonomy_name
    FROM
        {full_snapshot_table_id},
        # policy_tags column is an ARRAY of tag names; UNNEST expands the array
        # into rows.
        UNNEST(policy_tags) as policy_tag_name
    """

    logger.info(
        "Querying unique taxonomy names from %s in region %s...",
        full_snapshot_table_id,
        source_region,
    )
    try:
        query_job = bq_client.query(sql, location=source_region)
        results = query_job.result()
        taxonomy_names = [row.taxonomy_name for row in results]
        logger.info("Found %d unique taxonomies:", len(taxonomy_names))
        for taxonomy_name in taxonomy_names:
            logger.info("  - %s", taxonomy_name)
        return taxonomy_names
    except Exception as e:
        logger.exception("Error querying unique taxonomy names: %s", e)
        raise

def _get_source_taxonomy_names(
    replicate_all_taxonomies: bool,
    ptm_client: datacatalog_v1.PolicyTagManagerClient,
    bq_client: bigquery.Client,
    project_id: str,
    source_region: str,
    timestamp_str: str,
    project_qualified_bindings_dataset: str,
) -> List[str]:
    """Helper to get list of source taxonomy names based on replication mode."""
    if replicate_all_taxonomies:
        logger.info("Replication mode: Replicating all taxonomies in project %s", project_id)
        return _list_all_taxonomies(
            ptm_client, project_id, source_region
        )
    else:
        logger.info("Replication mode: Replicating taxonomies from snapshot %s", timestamp_str)
        return _get_taxonomies_from_snapshot_table(
            bq_client,
            project_id,
            source_region,
            timestamp_str,
            project_qualified_bindings_dataset,
        )

def replicate_taxonomies(
    bq_client: bigquery.Client,
    ptm_client: datacatalog_v1.PolicyTagManagerClient,
    policy_tag_manager_serialization_client: datacatalog_v1.PolicyTagManagerSerializationClient,
    project_id: str,
    source_region: str,
    destination_region: str,
    timestamp_str: str,
    project_qualified_bindings_dataset: str,
    replicate_all_taxonomies: bool,
) -> None:
    """Step 2: Replicate Taxonomy tree by creating new taxonomies with region suffix in the destination.

    This function exports taxonomies from the source region based on the policy
    tags found in the snapshot table. It then imports these taxonomies into the
    destination region, appending the destination region name to their display
    names to avoid conflicts.

    Args:
        bq_client: The BigQuery client.
        ptm_client: The Data Catalog PolicyTagManagerClient.
        policy_tag_manager_serialization_client: The Data Catalog
            PolicyTagManagerSerializationClient.
        project_id: The Google Cloud project ID.
        source_region: The source region of the BigQuery dataset.
        destination_region: The destination region for the replicated taxonomies.
        timestamp_str: A timestamp string in YYYYMMDDHHMMSS format, used to
            identify the snapshot table.
        project_qualified_bindings_dataset: Project-qualified dataset for
            bindings table.
        replicate_all_taxonomies: If true, replicate all taxonomies in
            project and source region.

    Raises:
        ValueError: If required arguments are missing.
        google.api_core.exceptions.AlreadyExists: If a taxonomy with the
            suffixed display name already exists in the destination region.
        Exception: For other errors during export or import.
    """
    logger.info("--- [Started] Step 2: Replicating Taxonomies ---")
    if not source_region:
        raise ValueError("--source_region is required for Step 2")
    if not destination_region:
        raise ValueError("--destination_region is required for Step 2")
    if not replicate_all_taxonomies and not timestamp_str:
        raise ValueError("--policy_tag_bindings_snapshot_timestamp is required for Step 2 when --replicate_all_taxonomies is not used")

    taxonomies_to_replicate_list = _get_source_taxonomy_names(
        replicate_all_taxonomies,
        ptm_client,
        bq_client,
        project_id,
        source_region,
        timestamp_str,
        project_qualified_bindings_dataset,
    )

    if not taxonomies_to_replicate_list:
        logger.info("No taxonomies found to replicate.")
        logger.info("--- [Finished] Step 2: Replicating Taxonomies ---\n")
        return

    if replicate_all_taxonomies:
        taxonomies_by_project = {project_id: taxonomies_to_replicate_list}
    else:
        taxonomies_by_project = _group_by_project(taxonomies_to_replicate_list)

    logger.info("Found taxonomies to replicate: %s", taxonomies_to_replicate_list)

    for taxonomy_project, taxonomy_names in taxonomies_by_project.items():
        logger.info("Processing %d taxonomies from project %s", len(taxonomy_names), taxonomy_project)
        # Export Taxonomies from source region
        export_parent = f"projects/{taxonomy_project}/locations/{source_region}"
        export_request = datacatalog_v1.ExportTaxonomiesRequest(
            parent=export_parent,
            taxonomies=taxonomy_names,
            serialized_taxonomies=True,
        )

        logger.info("Exporting taxonomies from %s in project %s...", source_region, taxonomy_project)
        try:
            exported_taxonomies = (
                policy_tag_manager_serialization_client.export_taxonomies(
                    request=export_request, retry=DEFAULT_RETRY
                )
            )
            logger.info(
                "Successfully exported %d taxonomies from %s in project %s.",
                len(exported_taxonomies.taxonomies),
                source_region,
                taxonomy_project,
            )
            logger.info("Export taxonomies response: %s", exported_taxonomies)
        except Exception as e:
            logger.exception("Error during export for project %s: %s", taxonomy_project, e)
            raise

        # Modify display names for the destination region
        for taxonomy in exported_taxonomies.taxonomies:
            original_display_name = taxonomy.display_name
            new_display_name = f"{original_display_name} - {destination_region}"
            # Add region suffix to display name to avoid naming conflicts if a
            # taxonomy with the same name already exists in the destination region
            # or if multiple source regions are migrated to the same destination.
            taxonomy.display_name = new_display_name
            logger.info(
                "Renaming taxonomy for destination: '%s' -> '%s'",
                original_display_name,
                taxonomy.display_name,
            )

        # Import new taxonomies to the destination region
        dest_parent = f"projects/{taxonomy_project}/locations/{destination_region}"
        import_request = datacatalog_v1.ImportTaxonomiesRequest(
            parent=dest_parent,
            inline_source=datacatalog_v1.InlineSource(taxonomies=exported_taxonomies.taxonomies),
        )

        logger.info("Importing %d taxonomies to %s in project %s...", len(exported_taxonomies.taxonomies), destination_region, taxonomy_project)
        try:
            import_response = policy_tag_manager_serialization_client.import_taxonomies(request=import_request, retry=DEFAULT_RETRY)
            logger.info(
                "Successfully imported %d taxonomies to project %s.",
                len(import_response.taxonomies),
                taxonomy_project,
            )
            logger.info("Import taxonomies response: %s", import_response)
        except api_core_exceptions.AlreadyExists as ae:
            logger.error("Error importing taxonomies to project %s: One or more taxonomies with the suffixed names already exist in %s. Please check the logs. Details: %s", taxonomy_project, destination_region, ae)
        except Exception as e:
            logger.exception("Error importing new taxonomies to project %s: %s", taxonomy_project, e)
            raise

    logger.info("--- [Finished] Step 2: Replicating Taxonomies ---\n")

def _get_all_policy_tags(ptm_client: datacatalog_v1.PolicyTagManagerClient, taxonomy_name: str) -> List[datacatalog_v1.PolicyTag]:
    """Helper function to list all policy tags in a taxonomy."""
    tags = []
    page_token = None
    # Paginate through all results from list_policy_tags API.
    logger.info("Listing policy tags for taxonomy: %s", taxonomy_name)
    while True:
        request = datacatalog_v1.ListPolicyTagsRequest(parent=taxonomy_name, page_token=page_token)
        response = ptm_client.list_policy_tags(request=request, retry=DEFAULT_RETRY)
        tags.extend(response.policy_tags)
        page_token = response.next_page_token
        if not page_token:
            break
    logger.info("Found %d policy tags: %s", len(tags), [tag.display_name for tag in tags])
    return tags

def replicate_policy_tag_iam_policies(
    bq_client: bigquery.Client,
    ptm_client: datacatalog_v1.PolicyTagManagerClient,
    project_id: str,
    source_region: str,
    destination_region: str,
    timestamp_str: str,
    project_qualified_bindings_dataset: str,
    replicate_all_taxonomies: bool,
) -> None:
    """Step 3: Replicate IAM policies for the Policy Tags.

    This function fetches the IAM policies from policy tags in the source region
    that are identified in the snapshot. It then applies these same IAM policies
    to the corresponding, newly created policy tags in the destination region.
    The mapping between source and destination policy tags is established using
    their display name paths within their respective taxonomies.

    Args:
        bq_client: The BigQuery client.
        ptm_client: The Data Catalog PolicyTagManagerClient.
        project_id: The Google Cloud project ID.
        source_region: The source region of the BigQuery dataset.
        destination_region: The destination region where IAM policies will be replicated.
        timestamp_str: A timestamp string in YYYYMMDDHHMMSS format, used to
            identify the snapshot table.
        project_qualified_bindings_dataset: Project-qualified dataset for
            bindings table.
        replicate_all_taxonomies: If true, replicate IAM policies for all
            taxonomies in project and source region.

    Raises:
        ValueError: If required arguments are missing.
        google.api_core.exceptions.NotFound: If a policy tag is not found during
            IAM operations.
        Exception: For other errors during API calls.
    """
    logger.info("--- [Started] Step 3: Replicating Policy Tag IAM Policies ---")
    if not source_region:
        raise ValueError("--source_region is required for Step 3")
    if not destination_region:
        raise ValueError("--destination_region is required for Step 3")
    if not replicate_all_taxonomies and not timestamp_str:
        raise ValueError("--policy_tag_bindings_snapshot_timestamp is required for Step 3 when --replicate_all_taxonomies is not used")

    unique_taxonomy_names_source = _get_source_taxonomy_names(
        replicate_all_taxonomies,
        ptm_client,
        bq_client,
        project_id,
        source_region,
        timestamp_str,
        project_qualified_bindings_dataset,
    )

    if not unique_taxonomy_names_source:
        logger.info("No source taxonomies found based on snapshot.")
        logger.info("--- [Finished] Step 3: Replicating Policy Tag IAM Policies ---\n")
        return

    logger.info("Building source to destination tag map for IAM replication...")
    src_to_dest_map = _build_src_to_dest_taxonomy_and_tags_map(
        ptm_client, project_id, source_region, destination_region, unique_taxonomy_names_source
    )

    if not src_to_dest_map:
        logger.error("Could not map source taxonomies to destination. Skipping IAM replication.")
        return

    for source_taxonomy_name, (destination_taxonomy_name, tag_map) in src_to_dest_map.items():
        logger.info("Syncing IAM policies for tags from %s to %s", source_taxonomy_name, destination_taxonomy_name)
        for src_tag_name, dest_tag_name in tag_map.items():
            logger.info("  Processing tag mapping: %s -> %s", src_tag_name, dest_tag_name)
            try:
                # 1. Get IAM policy from source tag.
                logger.info("    Getting IAM policy for source tag: %s", src_tag_name)
                get_iam_request_src = iam_policy_pb2.GetIamPolicyRequest(resource=src_tag_name)
                source_iam_policy = ptm_client.get_iam_policy(request=get_iam_request_src, retry=DEFAULT_RETRY)
                logger.info("      Source IAM Policy: %s", source_iam_policy)

                if not source_iam_policy.bindings:
                    logger.info("    No IAM bindings found for source tag %s, skipping set.", src_tag_name)
                    continue

                # 2. Get current IAM policy of destination tag to get the etag.
                logger.info("    Getting current IAM policy for destination tag: %s to fetch ETag", dest_tag_name)
                get_iam_request_dest = iam_policy_pb2.GetIamPolicyRequest(resource=dest_tag_name)
                dest_iam_policy_current = ptm_client.get_iam_policy(request=get_iam_request_dest, retry=DEFAULT_RETRY)
                logger.info("      Destination Current IAM Policy: %s", dest_iam_policy_current)

                # 3. Construct the new policy for destination, copying bindings
                # from source and etag from current destination policy.
                new_dest_policy = policy_pb2.Policy()
                new_dest_policy.etag = dest_iam_policy_current.etag
                new_dest_policy.bindings.extend(source_iam_policy.bindings)
                logger.info("      New Destination IAM Policy: %s", new_dest_policy)

                # 4. Set the new IAM policy on the destination tag.
                logger.info("    Setting IAM policy for destination tag: %s", dest_tag_name)
                set_iam_request = iam_policy_pb2.SetIamPolicyRequest(
                    resource=dest_tag_name, policy=new_dest_policy
                )
                ptm_client.set_iam_policy(request=set_iam_request, retry=DEFAULT_RETRY)
                logger.info(
                    "    Successfully replicated IAM policy for tag %s to %s",
                    src_tag_name,
                    dest_tag_name,
                )
            except api_core_exceptions.NotFound as nfe:
                 logger.warning("    Policy tag not found during IAM operations: %s", nfe)
            except Exception as e:
                logger.exception(
                    "    Error replicating IAM policy for tag %s to %s: %s",
                    src_tag_name,
                    dest_tag_name,
                    e,
                )

    logger.info("--- [Finished] Step 3: Replicating Policy Tag IAM Policies ---\n")

def _parse_routine_resource_name(resource_name: str) -> Tuple[str, str, str]:
    """Parses project, dataset, and routine IDs from a routine resource name."""
    # Expected format: projects/project-id/datasets/dataset-id/routines/routine-id
    match = re.fullmatch(r"projects/([^/]+)/datasets/([^/]+)/routines/([^/]+)", resource_name)
    if not match:
        raise ValueError(f"Invalid routine resource name format: {resource_name}")
    return match.groups()

def _replicate_routine(
    bq_client: bigquery.Client,
    src_routine_name: str,
    destination_region: str,
    destination_routine_dataset_id: str,
) -> Optional[str]:
    """Gets a routine from source region and creates it in the destination dataset.

    Args:
        bq_client: The BigQuery client.
        src_routine_name: The resource name of the source routine
            (e.g., projects/p/datasets/d/routines/r).
        destination_region: The region where the routine should be created.
        destination_routine_dataset_id: The dataset ID in the destination region
            where the routine will be created.

    Returns:
        The resource name of the created or existing routine in the destination
        region if successful, otherwise None.
    """
    logger.info("Replicating custom masking routine %s to dataset %s in region %s", src_routine_name, destination_routine_dataset_id, destination_region)

    try:
        project_id, src_dataset_id, routine_id = _parse_routine_resource_name(src_routine_name)
    except ValueError as error:
        logger.error("  Error parsing source routine name: %s", error)
        return None

    # Format for bq_client.get_routine: project.dataset.routine
    src_routine_ref_str = f"{project_id}.{src_dataset_id}.{routine_id}"
    dest_routine_ref_str = f"{project_id}.{destination_routine_dataset_id}.{routine_id}"
    dest_routine_resource_name = f"projects/{project_id}/datasets/{destination_routine_dataset_id}/routines/{routine_id}"

    try:
        # 1. Get the source routine
        logger.info("  Fetching source routine: %s", src_routine_ref_str)
        src_routine = bq_client.get_routine(src_routine_ref_str, retry=DEFAULT_RETRY)
        logger.info("    Source routine details: %s", src_routine.to_api_repr())

        # 2. Ensure the destination dataset exists and is in the correct region
        dest_dataset_ref = bq_client.dataset(destination_routine_dataset_id, project=project_id)
        try:
            dest_dataset = bq_client.get_dataset(dest_dataset_ref, retry=DEFAULT_RETRY)
            # Ensure the dataset we are writing to is in the correct destination region.
            if dest_dataset.location.lower() != destination_region.lower():
                raise ValueError(
                    f"Destination routine dataset '{destination_routine_dataset_id}' exists but is in region "
                    f"'{dest_dataset.location}', expected '{destination_region}'."
                )
            logger.info("  Destination routine dataset '%s' found in region '%s'.", destination_routine_dataset_id, dest_dataset.location)
        except cloud_exceptions.NotFound:
            logger.info("  Destination routine dataset '%s' not found, creating in '%s'...", destination_routine_dataset_id, destination_region)
            # If dataset for routines doesn't exist in destination, create it.
            dest_dataset = bigquery.Dataset(dest_dataset_ref)
            dest_dataset.location = destination_region
            bq_client.create_dataset(dest_dataset, retry=DEFAULT_RETRY)
            logger.info("  Created destination routine dataset '%s'.", destination_routine_dataset_id)

        # 3. Prepare the new routine for the destination
        dest_routine = bigquery.Routine(dest_routine_ref_str)
        dest_routine.type_ = src_routine.type_
        dest_routine.language = src_routine.language
        dest_routine.arguments = src_routine.arguments
        dest_routine.body = src_routine.body
        dest_routine.return_type = src_routine.return_type
        dest_routine.description = src_routine.description
        if hasattr(src_routine, 'determinism_level'):
            dest_routine.determinism_level = src_routine.determinism_level
        dest_routine.imported_libraries = src_routine.imported_libraries
        dest_routine.data_governance_type = src_routine.data_governance_type

        # Copy all relevant properties from source to destination routine object.
        logger.info("    Destination routine object to create: %s", dest_routine.to_api_repr())

        # 4. Create the routine in the destination
        logger.info("  Creating routine '%s' in dataset '%s' (region %s)", routine_id, destination_routine_dataset_id, destination_region)
        created_routine = bq_client.create_routine(dest_routine, exists_ok=True, retry=DEFAULT_RETRY)
        logger.info("  Successfully ensured routine exists: %s", created_routine.path)
        if created_routine.data_governance_type:
            logger.info("    Replicated routine data_governance_type: %s", created_routine.data_governance_type)

        # Poll for routine availability before returning, to avoid race conditions
        # where the data policy creation might fail if the routine isn't fully propagated.
        logger.info("  Polling for routine availability for up to 30 seconds...")
        for i in range(6):
            try:
                bq_client.get_routine(dest_routine_ref_str, retry=DEFAULT_RETRY)
                logger.info("  Routine %s is available.", dest_routine_ref_str)
                return dest_routine_resource_name
            except api_core_exceptions.NotFound:
                if i < 5:
                    time.sleep(5)
                else:
                    logger.error("  Routine %s did not become available in time.", dest_routine_ref_str)
                    return None

    except api_core_exceptions.NotFound:
        logger.error("  Source routine %s not found.", src_routine_ref_str)
        return None
    except Exception as e:
        logger.exception("  Error replicating routine %s: %s", src_routine_name, e)
        return None

def replicate_data_policies(
    bq_client: bigquery.Client,
    ptm_client: datacatalog_v1.PolicyTagManagerClient,
    dps_client: bigquery_datapolicies_v1.DataPolicyServiceClient,
    project_id: str,
    source_region: str,
    destination_region: str,
    timestamp_str: str,
    destination_routine_dataset_id: str,
    project_qualified_bindings_dataset: str,
    replicate_all_taxonomies: bool,
) -> None:
    """Step 4: Replicate Data Policies and their IAM policies.

    This function identifies data policies in the source region that are linked
    to the policy tags being migrated (based on the snapshot). It then
    recreates these data policies in the destination region, linking them to
    the newly created policy tags in the destination. Custom masking routines
    used by data policies are also replicated to the specified
    `destination_routine_dataset_id`. Finally, the IAM policies from the source
    data policies are copied to their destination counterparts.

    Args:
        bq_client: The BigQuery client.
        ptm_client: The Data Catalog PolicyTagManagerClient.
        dps_client: The BigQuery DataPolicyServiceClient.
        project_id: The Google Cloud project ID.
        source_region: The source region of the BigQuery dataset.
        destination_region: The destination region for the replicated data policies.
        timestamp_str: A timestamp string in YYYYMMDDHHMMSS format, used to
            identify the snapshot table.
        destination_routine_dataset_id: The dataset ID in the destination region
            where replicated custom masking routines will be stored.
        project_qualified_bindings_dataset: Project-qualified dataset for
            bindings table.
        replicate_all_taxonomies: If true, replicate data policies for all
            taxonomies in project and source region.

    Raises:
        ValueError: If required arguments are missing.
        google.api_core.exceptions.GoogleAPIError: For errors during API calls
            (e.g., listing, creating, or setting IAM policies on data policies).
        Exception: For other errors during the replication process.
    """
    logger.info("--- [Started] Step 4: Replicating Data Policies ---")
    if not source_region:
        raise ValueError("--source_region is required for Step 4")
    if not destination_region:
        raise ValueError("--destination_region is required for Step 4")
    if not replicate_all_taxonomies and not timestamp_str:
        raise ValueError("--policy_tag_bindings_snapshot_timestamp is required for Step 4 when --replicate_all_taxonomies is not used")
    if not destination_routine_dataset_id:
        raise ValueError("--destination_custom_masking_routine_dataset is required for Step 4")

    unique_taxonomy_names_source = _get_source_taxonomy_names(
        replicate_all_taxonomies,
        ptm_client,
        bq_client,
        project_id,
        source_region,
        timestamp_str,
        project_qualified_bindings_dataset,
    )

    if not unique_taxonomy_names_source:
        logger.info("No source taxonomies found, skipping data policy replication.")
        logger.info("--- [Finished] Step 4: Replicating Data Policies ---\n")
        return

    taxonomies_by_project = _group_by_project(unique_taxonomy_names_source)

    # List relevant data policies in the source region
    relevant_data_policies = []
    for taxonomy_project, taxonomy_names in taxonomies_by_project.items():
        source_parent = f"projects/{taxonomy_project}/locations/{source_region}"
        for taxonomy_name in taxonomy_names:
            list_dp_filter = f'policy_tag:{taxonomy_name}*'
            logger.info("Listing data policies in %s with filter: %s", source_parent, list_dp_filter)
            try:
                # Data policies are regional, list policies in the source region that
                # are attached to any policy tag within the migrated taxonomies.
                request = bigquery_datapolicies_v1.ListDataPoliciesRequest(
                    parent=source_parent, filter=list_dp_filter
                )
                response = dps_client.list_data_policies(request=request, retry=DEFAULT_RETRY)
                new_policies = list(response)
                if new_policies:
                    logger.info("  Found data policies: %s", [dp.name for dp in new_policies])
                relevant_data_policies.extend(new_policies)
            except Exception as e:
                logger.exception("Error listing data policies in source region with filter %s: %s", list_dp_filter, e)
                raise

    logger.info("Found %d data policies linked to the taxonomies to be migrated: %s", len(relevant_data_policies), [dp.name for dp in relevant_data_policies])

    if not relevant_data_policies:
        logger.info("No relevant data policies to replicate.")
        logger.info("--- [Finished] Step 4: Replicating Data Policies ---\n")
        return

    logger.info("Building source to destination tag map for data policy replication...")
    src_to_dest_map = _build_src_to_dest_taxonomy_and_tags_map(
        ptm_client, project_id, source_region, destination_region, unique_taxonomy_names_source
    )

    if not src_to_dest_map:
        logger.error("Could not map source taxonomies to destination. Skipping data policy replication.")
        return

    # Replicate each relevant data policy
    for src_dp in relevant_data_policies:
        logger.info("Processing source data policy: %s", src_dp.name)
        logger.info("  Source Data Policy details: %s", src_dp)

        src_policy_tag_name = src_dp.policy_tag
        source_taxonomy_name = "/".join(src_policy_tag_name.split("/")[:6])
        dest_policy_tag_name = None

        if source_taxonomy_name in src_to_dest_map:
            _, tag_map = src_to_dest_map[source_taxonomy_name]
            dest_policy_tag_name = tag_map.get(src_policy_tag_name)

        try:
            if not dest_policy_tag_name:
                logger.warning(
                    "  Destination policy tag for source tag %s not found in destination."
                    " Skipping data policy %s.",
                    src_policy_tag_name,
                    src_dp.name,
                )
                continue
            logger.info("  Mapped source tag %s to destination tag %s", src_policy_tag_name, dest_policy_tag_name)

            # Prepare new DataPolicy for creation
            new_data_policy = bigquery_datapolicies_v1.DataPolicy()
            new_data_policy.data_policy_type = src_dp.data_policy_type
            new_data_policy.data_policy_id = src_dp.data_policy_id
            new_data_policy.policy_tag = dest_policy_tag_name

            if src_dp.data_masking_policy:
                masking_policy = src_dp.data_masking_policy
                if masking_policy.predefined_expression:
                    new_data_policy.data_masking_policy = bigquery_datapolicies_v1.DataMaskingPolicy(
                        predefined_expression=masking_policy.predefined_expression
                    )
                    logger.info("    Using predefined expression: %s", masking_policy.predefined_expression)
                elif masking_policy.routine:
                    src_routine_name = masking_policy.routine
                    logger.info("    Data policy uses custom routine: %s", src_routine_name)
                    # Replicate routine and get the new name in the destination
                    dest_routine_name = _replicate_routine(bq_client, src_routine_name, destination_region, destination_routine_dataset_id)
                    if not dest_routine_name:
                        logger.error("    Failed to replicate routine %s, skipping data policy %s", src_routine_name, src_dp.name)
                        continue
                    new_data_policy.data_masking_policy = bigquery_datapolicies_v1.DataMaskingPolicy(
                        routine=dest_routine_name
                    )
                    logger.info("    Using replicated routine: %s", dest_routine_name)
                else:
                     logger.warning("    Data masking policy on %s has no predefined_expression or routine.", src_dp.name)
                     continue
            # Removed the incorrect 'data_governance_tag' check

            data_policy_project = _get_project_id_or_number(src_dp.name)
            dest_parent = f"projects/{data_policy_project}/locations/{destination_region}"
            create_request = bigquery_datapolicies_v1.CreateDataPolicyRequest(
                parent=dest_parent, data_policy=new_data_policy
            )

            try:
                # Attempt to create the data policy in the destination region.
                logger.info("  Creating data policy '%s' in %s", new_data_policy.data_policy_id, dest_parent)
                created_dp = dps_client.create_data_policy(request=create_request, retry=DEFAULT_RETRY)
                dest_dp_name = created_dp.name
                logger.info("  Successfully created data policy: %s", dest_dp_name)
            except api_core_exceptions.AlreadyExists:
                dest_dp_name = f"{dest_parent}/dataPolicies/{new_data_policy.data_policy_id}"
                logger.info("  Data policy %s already exists in %s. Will attempt to sync IAM.", dest_dp_name, dest_parent)
                # If data policy exists, we assume it's correctly configured
                # and only sync IAM policies.
            except Exception as e:
                logger.exception("  Error creating data policy %s: %s", new_data_policy.data_policy_id, e)
                continue

            # Replicate IAM policy for the data policy
            try:
                # Replicate IAM policy from source to destination data policy.
                logger.info("  Getting IAM policy for source data policy: %s", src_dp.name)
                get_iam_request_src = iam_policy_pb2.GetIamPolicyRequest(resource=src_dp.name)
                source_iam_policy = dps_client.get_iam_policy(request=get_iam_request_src, retry=DEFAULT_RETRY)

                if source_iam_policy.bindings:
                    logger.info("    Source Data Policy IAM: %s", source_iam_policy)
                    # Get destination etag before setting policy.
                    logger.info("  Getting current IAM policy for destination data policy: %s", dest_dp_name)
                    get_iam_request_dest = iam_policy_pb2.GetIamPolicyRequest(resource=dest_dp_name)
                    dest_iam_policy_current = dps_client.get_iam_policy(request=get_iam_request_dest, retry=DEFAULT_RETRY)

                    new_dest_iam_policy = policy_pb2.Policy()
                    new_dest_iam_policy.etag = dest_iam_policy_current.etag
                    new_dest_iam_policy.bindings.extend(source_iam_policy.bindings)

                    logger.info("  Setting IAM policy for destination data policy: %s", dest_dp_name)
                    set_iam_request = iam_policy_pb2.SetIamPolicyRequest(
                        resource=dest_dp_name, policy=new_dest_iam_policy
                    )
                    dps_client.set_iam_policy(request=set_iam_request, retry=DEFAULT_RETRY)
                    logger.info("  Successfully replicated IAM policy for data policy %s", dest_dp_name)
                else:
                    logger.info("  No IAM bindings on source data policy %s to replicate.", src_dp.name)

            except Exception as e:
                logger.exception("  Error replicating IAM policy for data policy %s: %s", dest_dp_name, e)

        except Exception as e:
            logger.exception("Error processing data policy %s: %s", src_dp.name, e)

    logger.info("--- [Finished] Step 4: Replicating Data Policies ---\n")

def promote_replica(
    bq_client: bigquery.Client,
    project_id: str,
    source_region: str,
    destination_region: str,
    dataset: str,
) -> None:
    """Step 5: Promote destination replica to primary.

    This function executes an ALTER SCHEMA statement to change the primary
    replica of a BigQuery dataset from the source region to the destination region.
    A critical warning is logged before proceeding, as this step will temporarily
    make policy-tagged columns inaccessible in both regions until Step 6 is run.

    Args:
        bq_client: The BigQuery client.
        project_id: The Google Cloud project ID.
        source_region: The current primary region of the dataset (e.g., "us").
        destination_region: The region to promote to primary (e.g., "us-east4").
        dataset: The ID of the dataset to be promoted.

    Raises:
        ValueError: If any required arguments are missing.
        Exception: If an error occurs during the BigQuery query execution.
    """
    logger.info("--- [Started] Step 5: Promoting Replica to Primary ---")
    if not source_region:
        raise ValueError("--source_region is required for Step 5")
    if not destination_region:
        raise ValueError("--destination_region is required for Step 5")
    if not dataset:
        raise ValueError("--dataset is required for Step 5")

    logger.warning(
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! CRITICAL WARNING"
        " !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    logger.warning(
        "After this promotion, columns with policy tags in the specified dataset")
    logger.warning(
        "WILL BECOME INACCESSIBLE in BOTH the source region ('%s')", source_region)
    logger.warning(
        "and the destination region ('%s') until Step 6 is run.",
        destination_region,
    )
    logger.warning(
        "This is because the policy tag linkages need to be updated in the new"
        " primary.")
    logger.warning(
        "It is HIGHLY RECOMMENDED to run Step 6 immediately after this step.")
    logger.warning(
        "This promotion can be reverted by running: ALTER SCHEMA `%s.%s` SET OPTIONS(primary_replica = '%s')",
        project_id, dataset, source_region)
    logger.warning(
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    full_dataset_id = f"{project_id}.{dataset}"
    # Correct SQL for promoting replica
    sql = f"ALTER SCHEMA `{full_dataset_id}` SET OPTIONS(primary_replica = '{destination_region}')"

    logger.info(
        "The following SQL statement will be executed in region %s to promote the replica:",
        destination_region,
    )
    logger.info(sql)

    input(
        f"Press Enter to proceed with promotion for {full_dataset_id}, "
        "or Ctrl+C to cancel...")
    logger.info(
        "Promoting replica for dataset: %s, setting %s as primary",
        full_dataset_id,
        destination_region,
    )
    try:
        # Promotion is an ALTER SCHEMA statement run in the DESTINATION region
        query_job = bq_client.query(sql, location=destination_region)
        query_job.result()  # Wait for the job to complete
        logger.info(
            "Successfully promoted replica for %s. Primary is now %s.",
            full_dataset_id,
            destination_region,
        )
        logger.info("*** IMPORTANT: Waiting 2 minutes to allow primary replica switch to propagate before finishing Step 5... ***")
        time.sleep(120)
    except Exception as e:
        logger.exception("Error promoting replica for %s: %s", full_dataset_id, e)
        raise
    logger.info("--- [Finished] Step 5: Promoting Replica to Primary ---\n")

def _get_tag_path(tag_name: str, tag_map: Dict[str, datacatalog_v1.PolicyTag]) -> str:
    """Helper to compute '.' separated display name path for a policy tag."""
    path = []
    curr_tag_name = tag_name
    # Traverse from the current tag up to its parent recursively
    # until the root of the taxonomy is reached.
    # This builds a path like "ParentTag.ChildTag.GrandchildTag".
    while curr_tag_name in tag_map:
        tag = tag_map[curr_tag_name]
        path.insert(0, tag.display_name)
        curr_tag_name = tag.parent_policy_tag
    return ".".join(path)

def _build_src_to_dest_taxonomy_and_tags_map(
    ptm_client: datacatalog_v1.PolicyTagManagerClient,
    project_id: str,
    source_region: str,
    destination_region: str,
    source_taxonomy_names: List[str],
) -> Dict[str, Tuple[str, Dict[str, str]]]:
    """
    Builds a map of source taxonomy and policy tag names to their destination counterparts.

    The map structure is:
    {
        "source_taxonomy_name": (
            "destination_taxonomy_name",
            {
                "source_policy_tag_1_name": "dest_policy_tag_1_name",
                "source_policy_tag_2_name": "dest_policy_tag_2_name",
            }
        ), ...
    }
    Policy tags are matched using their full display name path to handle hierarchies correctly.
    """
    src_to_dest_map = {}
    # Cache of {project_id: {display_name: taxonomy}} for destination region
    dest_taxonomies_cache: Dict[str, Dict[str, datacatalog_v1.Taxonomy]] = {}

    for source_taxonomy_name in source_taxonomy_names:
        try:
            taxonomy_project = _get_project_id_or_number(source_taxonomy_name)

            if taxonomy_project not in dest_taxonomies_cache:
                # This project's dest taxonomies not yet listed. List them now.
                dest_parent = f"projects/{taxonomy_project}/locations/{destination_region}"
                logger.info("Listing existing taxonomies in %s for mapping...", dest_parent)
                project_dest_taxonomies = {}
                try:
                    for taxonomy in ptm_client.list_taxonomies(parent=dest_parent, retry=DEFAULT_RETRY):
                        project_dest_taxonomies[taxonomy.display_name] = taxonomy
                    dest_taxonomies_cache[taxonomy_project] = project_dest_taxonomies
                    logger.info("Found %d taxonomies in %s.", len(project_dest_taxonomies), dest_parent)
                except Exception as e:
                    logger.exception("Error listing taxonomies in %s for mapping: %s", dest_parent, e)
                    # Cache empty on error so we don't retry listing for this project.
                    dest_taxonomies_cache[taxonomy_project] = {}

            if not dest_taxonomies_cache.get(taxonomy_project):
                logger.warning("Cannot map taxonomy %s, failed to list destination taxonomies in project %s.", source_taxonomy_name, taxonomy_project)
                continue

            src_taxonomy = ptm_client.get_taxonomy(name=source_taxonomy_name, retry=DEFAULT_RETRY)
            source_display_name = src_taxonomy.display_name

            # Find destination taxonomy by matching display name.
            dest_display_name = f"{source_display_name} - {destination_region}"
            dest_taxonomy = dest_taxonomies_cache[taxonomy_project].get(dest_display_name)

            if dest_taxonomy:
                destination_taxonomy_name = dest_taxonomy.name
                logger.info("Mapping source taxonomy %s to dest taxonomy %s", source_taxonomy_name, destination_taxonomy_name)
                src_tags = _get_all_policy_tags(ptm_client, source_taxonomy_name)
                dest_tags = _get_all_policy_tags(ptm_client, destination_taxonomy_name)
                # Create maps of name -> tag object for efficient path lookup.
                src_tag_map_dict = {t.name: t for t in src_tags}
                dest_tag_map_dict = {t.name: t for t in dest_tags}

                # For destination tags, create a map of path -> name for matching.
                dest_tags_by_path = {
                    _get_tag_path(t.name, dest_tag_map_dict): t.name for t in dest_tags
                }

                # For each source tag, find its path, and use the path to find
                # the matching destination tag resource name.
                tag_map = {}
                for src_tag in src_tags:
                    src_path = _get_tag_path(src_tag.name, src_tag_map_dict)
                    dest_tag_name = dest_tags_by_path.get(src_path)
                    if dest_tag_name:
                        tag_map[src_tag.name] = dest_tag_name
                    else:
                         logger.warning("No match for source tag %s (path %s) in destination taxonomy %s", src_tag.name, src_path, destination_taxonomy_name)
                src_to_dest_map[source_taxonomy_name] = (destination_taxonomy_name, tag_map)
            else:
                logger.warning("Could not find destination taxonomy for source %s (expected: %s)", source_taxonomy_name, dest_display_name)
        except Exception as e:
            logger.exception("Error mapping source taxonomy %s: %s", source_taxonomy_name, e)
            continue
    logger.info("src_to_dest_map: %s", src_to_dest_map)
    return src_to_dest_map

def _apply_policy_tags_to_schema(
    schema: List[bigquery.SchemaField],
    binding_map: Dict[str, List[str]],
    get_dest_policy_tag_name_func,
) -> Tuple[List[bigquery.SchemaField], bool]:
    """
    Applies policy tags from bindings to a schema, by rebuilding the schema fields.
    Returns (new_schema, changed).
    """
    schema_changed = False

    # Recursively traverses schema fields to update policy tags.
    def rebuild_fields(fields: Tuple[bigquery.SchemaField, ...], prefix: str) -> Tuple[bigquery.SchemaField, ...]:
        nonlocal schema_changed
        new_fields = []
        for field in fields:
            current_path = f"{prefix}{field.name}" if prefix else field.name

            new_subfields = field.fields
            # If the field is a STRUCT/RECORD, recurse into its subfields.
            if field.fields:
                new_subfields = rebuild_fields(tuple(field.fields), f"{current_path}.")

            # Get policy tags for this field path from the snapshot.
            snapshot_policy_tags = binding_map.get(current_path)
            current_policy_tag_names = sorted(field.policy_tags.names if field.policy_tags else [])
            dest_policy_tag_names_list = []
            target_policy_tags = field.policy_tags

            # Per user request: ONLY update policyTags for a column if there were
            # policyTags on that column in the schema already and containing 'taxonomies/0'.
            if field.policy_tags and any('taxonomies/0/policyTags' in pt for pt in current_policy_tag_names):
                if snapshot_policy_tags:
                    # If tags are found in snapshot, map them to destination tag names.
                    for source_policy_tag in snapshot_policy_tags:
                        dest_policy_tag = get_dest_policy_tag_name_func(source_policy_tag)
                        if dest_policy_tag:
                            dest_policy_tag_names_list.append(dest_policy_tag)
                        else:
                            logger.warning("    Failed to find destination tag for %s on field %s", source_policy_tag, current_path)
                    dest_policy_tag_names_list.sort()

                # If calculated destination tags differ from current tags, update is needed.
                if current_policy_tag_names != dest_policy_tag_names_list:
                    schema_changed = True
                    logger.info("  Updating policy tags for field: %s: %s -> %s", current_path, current_policy_tag_names, dest_policy_tag_names_list)
                    if dest_policy_tag_names_list:
                        target_policy_tags = bigquery.schema.PolicyTagList(names=dest_policy_tag_names_list)
                    else:
                        target_policy_tags = None
            elif field.policy_tags:
                 logger.info("Skipping field %s as it has policy tags but none contain 'taxonomies/0/'", current_path)
            elif snapshot_policy_tags:
                 logger.info("Skipping field %s as it has no policy tags in destination, but is in snapshot.", current_path)

            # Build kwargs for SchemaField, omitting None values for optional fields
            # to prevent them appearing as `null` in the schema diff and API calls.
            sf_kwargs = {}
            if field.description:
                sf_kwargs['description'] = field.description
            if target_policy_tags:
                sf_kwargs['policy_tags'] = target_policy_tags
            if new_subfields:
                sf_kwargs['fields'] = new_subfields

            new_fields.append(
                 bigquery.SchemaField(
                    name=field.name,
                    field_type=field.field_type,
                    mode=field.mode,
                    **sf_kwargs)
            )
        return tuple(new_fields)

    top_level_fields = tuple(schema)
    rebuilt_schema = list(rebuild_fields(top_level_fields, ""))
    return rebuilt_schema, schema_changed

def _backup_schema(table: bigquery.Table, backup_dir: str, timestamp_str: str) -> None:
    """Saves the current table schema to a JSON file."""
    file_name = f"{table.project}.{table.dataset_id}.{table.table_id}.{timestamp_str}.schema.json"
    file_path = os.path.join(backup_dir, file_name)

    schema_api = [f.to_api_repr() for f in table.schema]
    with open(file_path, 'w') as f:
        json.dump(schema_api, f, indent=2)
    logger.info(f"  Successfully backed up schema for {table.reference} to {file_path}")

def update_table_schemas(
    bq_client: bigquery.Client,
    ptm_client: datacatalog_v1.PolicyTagManagerClient,
    project_id: str,
    source_region: str,
    destination_region: str,
    dataset: Optional[str],
    all_datasets: bool,
    timestamp_str: str,
    project_qualified_bindings_dataset: str,
    skip_confirmation: bool = False,
) -> None:
    """Step 6: Update Table Schemas in the new primary region.

    This function reads the policy tag bindings from the snapshot table,
    maps the source policy tags to their newly created counterparts in the
    destination region, and then updates the BigQuery table schemas in the
    destination region to point to these new policy tags.

    Args:
        bq_client: The BigQuery client.
        ptm_client: The Data Catalog PolicyTagManagerClient.
        project_id: The Google Cloud project ID.
        source_region: The original source region of the dataset.
        destination_region: The new primary region where schemas will be updated.
        dataset: The ID of the dataset containing the tables to update.
        all_datasets: If True, update schemas for all tables in snapshot.
        timestamp_str: A timestamp string (YYYYMMDDHHMMSS) used to identify
            the policy tag bindings snapshot table.
        project_qualified_bindings_dataset: Project-qualified dataset for
            bindings table.

    Raises:
        ValueError: If required arguments are missing.
        Exception: For errors during BigQuery or Data Catalog API calls.
    """
    logger.info("--- [Started] Step 6: Updating Table Schemas ---")
    if not source_region:
        raise ValueError("--source_region is required for Step 6")
    if not destination_region:
        raise ValueError("--destination_region is required for Step 6")
    if not dataset and not all_datasets:
        raise ValueError("Either --dataset or --all_datasets must be specified for Step 6")
    if dataset and all_datasets:
        raise ValueError("--dataset and --all_datasets cannot be specified together for Step 6")
    if not timestamp_str:
        raise ValueError(
            "--policy_tag_bindings_snapshot_timestamp is required for Step 6"
        )

    snapshot_table_id = f"{BINDINGS_TABLE_PREFIX}{timestamp_str}"
    full_snapshot_table_id = (
        f"`{project_qualified_bindings_dataset}.{snapshot_table_id}`"
    )

    where_clause = ""
    if not all_datasets:
        where_clause = f"WHERE dataset_id = '{dataset}'"

    # Query the snapshot table to get all column/tag bindings for the
    # datasets being processed.
    query = f"""
    SELECT
        project_id,
        dataset_id,
        table_name,
        column_name,
        field_path,
        policy_tags
    FROM {full_snapshot_table_id}
    {where_clause}
    ORDER BY project_id, dataset_id, table_name, column_name, field_path
    """
    logger.info("Querying snapshot table for policy tag bindings...")
    logger.info("Snapshot query:\n%s", query)
    try:
        # Snapshot table is in the source region
        query_job = bq_client.query(query, location=source_region)
        snapshot_rows = list(query_job.result())
        logger.info("Found %d column/field policy tag bindings in snapshot.", len(snapshot_rows))
    except Exception as e:
        logger.exception("Error querying snapshot table: %s", e)
        return

    if not snapshot_rows:
        logger.info("No policy tag bindings found in snapshot for the given dataset.")
        logger.info("--- [Finished] Step 6: Updating Table Schemas ---\n")
        return

    # Extract unique source taxonomy names from all policy tags in the snapshot results.
    src_taxonomy_names = list(set(
        "/".join(pt.split("/")[:6])
        for row in snapshot_rows
        for pt in row["policy_tags"]
    ))
    # Build the map from source resource names to destination resource names
    # for taxonomies and policy tags.
    src_to_dest_map = _build_src_to_dest_taxonomy_and_tags_map(
        ptm_client, project_id, source_region, destination_region, src_taxonomy_names
    )
    logger.info("Source to Destination Taxonomy & Tag Map: %s", src_to_dest_map)

    if not src_to_dest_map:
        logger.error("Could not build source to destination taxonomy/tag map. Aborting schema updates.")
        return

    # Group snapshot rows by table for easier processing.
    table_bindings = {}
    for row in snapshot_rows:
        table_ref = f'{row["project_id"]}.{row["dataset_id"]}.{row["table_name"]}'
        if table_ref not in table_bindings:
            table_bindings[table_ref] = []
        table_bindings[table_ref].append(row)

    # Process each table
    for table_ref, bindings in table_bindings.items():
        logger.info("Processing table: %s", table_ref)

        try:
            # Fetch the current table schema from BigQuery.
            table = bq_client.get_table(table_ref, retry=DEFAULT_RETRY)
            original_schema_api = [f.to_api_repr() for f in table.schema]

            # Create a map for easy lookup of bindings by field path for the current table.
            binding_map = {b["field_path"]: b["policy_tags"] for b in bindings}

            # Helper function to look up destination tag name from a source tag name
            # using the previously built src_to_dest_map.
            def get_dest_policy_tag_name(source_policy_tag_name: str) -> Optional[str]:
                source_taxonomy_name = "/".join(source_policy_tag_name.split("/")[:6])
                if source_taxonomy_name in src_to_dest_map:
                    _, tag_map = src_to_dest_map[source_taxonomy_name]
                    return tag_map.get(source_policy_tag_name)
                return None

            # Calculate the new schema based on snapshot bindings and mapped tags.
            new_schema, schema_changed = _apply_policy_tags_to_schema(
                table.schema, binding_map, get_dest_policy_tag_name
            )

            if schema_changed:
                # If changes are detected, generate a diff for logging/review.
                new_schema_api = [f.to_api_repr() for f in new_schema]
                original_schema_json = json.dumps(original_schema_api, indent=2, sort_keys=True)
                new_schema_json = json.dumps(new_schema_api, indent=2, sort_keys=True)

                # Use difflib to show schema changes clearly.
                diff = difflib.unified_diff(
                    original_schema_json.splitlines(),
                    new_schema_json.splitlines(),
                    fromfile="original_schema",
                    tofile="new_schema",
                    lineterm="",
                )

                logger.info("  Schema changes for table %s:", table_ref)
                logger.info("  --- Schema Diff ---")
                for line in diff:
                    logger.info("  %s", line)
                logger.info("  --- End Diff ---")

                apply_changes = False
                if skip_confirmation:
                    logger.info(
                        "  --skip_confirmation_step6 provided, applying schema "
                        "changes automatically."
                    )
                    apply_changes = True
                else:
                    # Ask user for confirmation before applying schema changes.
                    prompt = f"  Apply these schema changes to {table_ref}? (y/N): "
                    confirm = input(prompt)
                    logger.info("  %s -> User response: %s", prompt, confirm)
                    if confirm.lower() == "y":
                        apply_changes = True

                if apply_changes:
                    logger.info("  Updating schema for %s...", table_ref)
                    table.schema = new_schema
                    bq_client.update_table(table, ["schema"], retry=DEFAULT_RETRY)
                    logger.info("  Successfully updated schema for %s.", table_ref)
                else:
                    logger.info("  Skipping schema update for %s.", table_ref)
            else:
                logger.info("  No schema changes detected for %s.", table_ref)

        except Exception as e:
            logger.exception("Error updating schema for table %s: %s", table_ref, e)

    logger.info("--- [Finished] Step 6: Updating Table Schemas ---\n")

def main():
    """Main function to orchestrate the policy tag migration steps."""
    run_timestamp = datetime.now()
    timestamp_str = run_timestamp.strftime("%Y%m%d%H%M%S")
    default_log_file = f"migrate_policy_tags_{timestamp_str}.log"

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--project_id", required=True, help="Your Google Cloud project ID."
    )
    parser.add_argument(
        "--source_region",
        help="The source region/multi-region (e.g., us).",
    )
    parser.add_argument(
        "--destination_region",
        help="The destination region (e.g., us-east4).",
    )
    parser.add_argument(
        "--dataset",
        help="Dataset ID to process.",
    )
    parser.add_argument(
        "--all_datasets",
        action="store_true",
        help="For Step 1, snapshot policy tag bindings for all datasets in "
             "project_id and source_region. For Step 6, update schemas for "
             "all tables in policy tag bindings snapshot.",
    )
    parser.add_argument(
        "--log_file",
        help=f"Path to the log file. Defaults to ./{default_log_file}",
        default=default_log_file,
    )
    parser.add_argument(
        "--log_level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (e.g., DEBUG, INFO). Default is INFO.",
    )
    parser.add_argument(
        "--policy_tag_bindings_snapshot_timestamp",
        help=(
            "Timestamp (YYYYMMDDHHMMSS) of the policy tag bindings table to use "
            " for steps 2, 3, 4, and 6. If omitted, a new timestamp is generated for Step 1."
            " If Step 1 is not run, this argument is required for Steps 2, 3, 4, or 6."
            " Note: This flag is ignored for steps 2, 3, and 4 if --replicate_all_taxonomies is used."
        ),
        default=None,
    )
    parser.add_argument(
        "--policy_tag_bindings_dataset",
        help="Dataset for policy tag bindings snapshot table, in 'dataset_id' or"
             " 'project_id.dataset_id' format. If provided, this dataset must"
             " already exist. If not provided, 'policy_tag_bindings_dataset'"
             " in the source region project will be used, and created if it"
             " doesn't exist.",
        default=None,
    )
    parser.add_argument(
        "--destination_custom_masking_routine_dataset",
        help="Dataset ID in the destination region to store replicated custom masking routines.",
    )
    parser.add_argument(
        "--table_schema_backup_dir",
        help="Directory to save table schema backups during Step 1. Required for Step 1.",
        default=None,
    )
    parser.add_argument(
        "--replicate_all_taxonomies",
        action="store_true",
        help="Replicate all taxonomies from project_id and source_region, "
             "ignoring policy tag binding snapshots for Steps 2, 3, and 4. "
             "If false, only taxonomies with policy tags found in Step 1 "
             "snapshot will be replicated.",
    )

    parser.add_argument(
        "--skip_confirmation_step6",
        action="store_true",
        help="If provided, skip manual confirmation before applying schema changes in Step 6.",
        default=False,
    )

    # Flags for each step
    parser.add_argument(
        "--step1",
        action="store_true",
        help="Run Step 1: Save column policy tag bindings.",
    )
    parser.add_argument(
        "--step2", action="store_true", help="Run Step 2: Replicate taxonomies."
    )
    parser.add_argument(
        "--step3",
        action="store_true",
        help="Run Step 3: Replicate policy tag IAM policies.",
    )
    parser.add_argument(
        "--step4",
        action="store_true",
        help="Run Step 4: Replicate data policies.",
    )
    parser.add_argument(
        "--step5",
        action="store_true",
        help="Run Step 5: Promote destination replica to primary.",
    )
    parser.add_argument(
        "--step6",
        action="store_true",
        help="Run Step 6: Update table schemas.",
    )

    args = parser.parse_args()
    log_level = logging.getLevelName(args.log_level.upper())
    _setup_logging(args.log_file, log_level)

    dataset = args.dataset.strip() if args.dataset else None

    any_step_flagged = any(
        [args.step1, args.step2, args.step3, args.step4, args.step5, args.step6]
    )

    if not any_step_flagged:
        parser.print_help()
        sys.exit(0)

    logger.info("Script execution started at: %s", run_timestamp.isoformat())
    logger.info("Running on project: %s", args.project_id)
    if args.source_region:
        logger.info("Source Region: %s", args.source_region)
    if args.destination_region:
        logger.info("Destination Region: %s", args.destination_region)
    if dataset:
        logger.info("Dataset to process: %s", dataset)
    if args.all_datasets:
        logger.info("The --all_datasets flag is set. This applies to Step 1 (snapshotting) and Step 6 (schema updates) when active.")
    if args.destination_custom_masking_routine_dataset:
        logger.info("Destination Routine Dataset: %s", args.destination_custom_masking_routine_dataset)
    if args.table_schema_backup_dir:
        logger.info("Table Schema Backup Directory: %s", args.table_schema_backup_dir)

    if args.policy_tag_bindings_dataset:
        bindings_dataset_arg = args.policy_tag_bindings_dataset
        if "." in bindings_dataset_arg:
            project_qualified_bindings_dataset = bindings_dataset_arg
        else:
            project_qualified_bindings_dataset = (
                f"{args.project_id}.{bindings_dataset_arg}"
            )
        create_bindings_dataset_if_not_exists = False
        logger.info(
            "Using user-provided policy tag bindings dataset: %s",
            project_qualified_bindings_dataset,
        )
    else:
        project_qualified_bindings_dataset = (
            f"{args.project_id}.{DEFAULT_BINDINGS_DATASET}"
        )
        create_bindings_dataset_if_not_exists = True
        logger.info(
            "Using default policy tag bindings dataset: %s",
            project_qualified_bindings_dataset,
        )

    snapshot_ts = args.policy_tag_bindings_snapshot_timestamp
    if args.step1:
        snapshot_ts = snapshot_ts or timestamp_str
    logger.info("Policy tag bindings snapshot timestamp: %s", snapshot_ts)

    try:
        bq_client = bigquery.Client(project=args.project_id)
        # Use PolicyTagManagerSerializationClient for export/import taxonomies
        policy_tag_manager_serialization_client = datacatalog_v1.PolicyTagManagerSerializationClient()
        # Use PolicyTagManagerClient for IAM policy operations on tags and general taxonomy operations
        ptm_client = datacatalog_v1.PolicyTagManagerClient()
        # Use DataPolicyServiceClient for Data Policy operations
        dps_client = bigquery_datapolicies_v1.DataPolicyServiceClient()

        if args.step1:
            save_policy_tag_bindings(
                bq_client,
                args.project_id,
                args.source_region,
                dataset,
                args.all_datasets,
                snapshot_ts,
                project_qualified_bindings_dataset,
                create_bindings_dataset_if_not_exists,
                args.table_schema_backup_dir,
            )
        if args.step2:
            replicate_taxonomies(
                bq_client,
                ptm_client,
                policy_tag_manager_serialization_client,
                args.project_id,
                args.source_region,
                args.destination_region,
                snapshot_ts,
                project_qualified_bindings_dataset,
                args.replicate_all_taxonomies,
            )
        if args.step3:
            replicate_policy_tag_iam_policies(
                bq_client,
                ptm_client,
                args.project_id,
                args.source_region,
                args.destination_region,
                snapshot_ts,
                project_qualified_bindings_dataset,
                args.replicate_all_taxonomies,
            )
        if args.step4:
            replicate_data_policies(
                bq_client,
                ptm_client,
                dps_client,
                args.project_id,
                args.source_region,
                args.destination_region,
                snapshot_ts,
                args.destination_custom_masking_routine_dataset,
                project_qualified_bindings_dataset,
                args.replicate_all_taxonomies,
            )
        if args.step5:
            promote_replica(
                bq_client,
                args.project_id,
                args.source_region,
                args.destination_region,
                dataset,
            )
        if args.step6:
            update_table_schemas(
                bq_client,
                ptm_client,
                args.project_id,
                args.source_region,
                args.destination_region,
                dataset,
                args.all_datasets,
                snapshot_ts,
                project_qualified_bindings_dataset,
                args.skip_confirmation_step6,
            )

    except ValueError as ve:
        logger.exception("Argument error: %s", ve)
        parser.print_help()
        sys.exit(1)
    except api_core_exceptions.GoogleAPIError as e:
        logger.exception("A Google Cloud API error occurred: %s", e)
        sys.exit(1)
    except (RuntimeError, TypeError, AttributeError, ImportError) as e:
        # Catch common unexpected runtime exceptions.
        logger.exception("An unexpected runtime error occurred: %s", e)
        sys.exit(1)
    except Exception as e:
        # Catch any other unforeseen exceptions.
        logger.exception("A general error occurred: %s", e)
        sys.exit(1)

    logger.info("Policy Tag Migration script execution finished.")
    if args.log_file:
        print(f"\nLog file saved to: {os.path.abspath(args.log_file)}")


if __name__ == "__main__":
  main()
