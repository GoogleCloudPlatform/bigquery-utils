# BigQuery Policy Tag Migration Script

## Disclaimer

***GOOGLE CONFIDENTIAL - PROVIDED FOR CVS HEALTH USE ONLY***

***AS IS, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED***

***NOT AN OFFICIALLY SUPPORTED GOOGLE PRODUCT***

**This script is provided on a best-effort basis to assist with the migration of
BigQuery Policy Tags and Data Policies between regions. It has not been
thoroughly tested, and Google provides no guarantee for its correctness. You
agree to use this script at your own risk.**

--------------------------------------------------------------------------------

## !! CRITICAL - PLEASE READ BEFORE USE !!

**REVIEW LOGS AT EACH STEP:**

It is **ABSOLUTELY ESSENTIAL** to carefully review the log output produced by
this script after **EVERY** step. Pay close attention to any messages prefixed
with `WARNING` or `ERROR`.

**DO NOT PROCEED IF ERRORS OCCUR:**

*   If you encounter any `ERROR` messages that you cannot resolve, or if
    `WARNING` messages indicate a potential issue, **DO NOT PROCEED** to the
    next step.
*   Retrying the current step might resolve transient issues.
*   Persistent errors or concerning warnings should be reported to your Google
    contact immediately.

**SECURITY IMPLICATIONS:**

*   Failure to replicate governance artifacts (like Data Policies or IAM
    bindings) correctly can lead to **security discrepancies** between the
    source and destination regions.
*   Promoting the replica to primary (Step 5) without ensuring all governance
    controls are mirrored can result in unintended data exposure or access
    denial in the new primary region.
*   **NEVER** proceed to Step 5 (Promote Replica) if you have any doubts about
    the successful completion of Steps 1-4.

**By using this script, you acknowledge the risks and agree to meticulously
check the logs at each stage.**

--------------------------------------------------------------------------------

## Overview

When a BigQuery dataset is replicated across regions (e.g., from 'us' to
'us-east4'), table schemas are replicated, but the Data Catalog Policy Tags
attached to columns are not fully functional in the destination region. The
policy tag resource IDs are retained in the schema, but they point to a
non-existent taxonomy (`taxonomy/0`) in the new region because policy tags are
regional resources. This can cause queries on tagged columns in the destination
region to fail with "Access Denied" errors, as fine-grained access control
cannot be resolved.

This script automates the process of recreating the governance artifacts
(Taxonomies, Policy Tag IAM Policies, Data Policies) in a destination region and
updating table schemas to link columns to the correct, newly replicated policy
tags in that region.

**NOTE:** The script has the following limitations:

*   It **does not** replicate any IAM permissions set on custom masking routines
    in the source region when such routines are replicated as part of data
    policy replication (Step 4); these permissions must also be manually
    reapplied in the destination region.

## Understanding Replication Modes

This script can operate in two modes for replicating taxonomies, policy tags,
and data policies (Steps 2, 3, and 4):

**1. Snapshot-Based Replication (Default)**

*   **Goal:** To replicate only the taxonomies and policy tags that are
    currently used by tables in a specific dataset, as captured by the Step 1
    snapshot.
*   **Use Case:** This is the **recommended mode** when migrating policy tags as
    part of a dataset replication/failover (i.e., running Steps 1-6). It ensures
    that Step 6 can correctly update table schemas using a consistent view of
    policy tags from the snapshot. This mode correctly handles cases where
    tables in the dataset reference policy tags from taxonomies located in
    **different projects**, as Step 1 snapshots all policy tags regardless of
    their project origin. The script will then attempt to replicate all
    taxonomies found in the snapshot during Step 2.
*   **How:** Run Steps 1-6 sequentially. For Steps 2, 3, 4 and 6, provide the
    `--policy_tag_bindings_snapshot_timestamp` from Step 1. **Do NOT use** the
    `--replicate_all_taxonomies` flag in this mode.

**2. Full Project Replication (`--replicate_all_taxonomies`)**

*   **Goal:** To replicate **all** taxonomies, policy tags, and associated data
    policies that exist in the `--project_id` and `--source_region`, regardless
    of whether they are used in any specific table or snapshot.
*   **Use Case:** Useful for syncing all definitions from a central "policy
    admin" project to a destination region, or ensuring all unused policy tags
    are also replicated. This mode should be run **independently** of Step 6, as
    it does not guarantee consistency with a Step 1 snapshot.
*   **How:** Run Steps 2, 3, and 4 with the `--replicate_all_taxonomies` flag.
    You do not need to provide `--policy_tag_bindings_snapshot_timestamp`.

> **Recommendation:**
>
> There are two independent ways to use this script:
>
> *   **(a) Migrating policy tags for dataset tables:** Use Snapshot-Based
>     Replication (Mode 1) and run steps 1-6 without the
>     `--replicate_all_taxonomies` flag. This ensures consistency for making
>     tables in the destination region operational.
> *   **(b) Replicating all taxonomies in a project:** If you need to replicate
>     *all* taxonomies (including unused ones), run Steps 2, 3, and 4 using Full
>     Project Replication (Mode 2) with the `--replicate_all_taxonomies` flag.
>     This can be done after a dataset migration is complete, or as a standalone
>     task.
>
> **Do NOT mix modes when migrating a dataset via steps 1-6.** Step 6 (`Update
> Table Schemas`) **always** relies on the Step 1 snapshot. You should NOT use
> `--replicate_all_taxonomies` when running steps 1-6 for dataset migration. The
> `--replicate_all_taxonomies` flag only replicates taxonomies from the single
> project specified in `--project_id`. If tables in your dataset reference
> policy tags from taxonomies in a *different* project, those taxonomies will
> not be replicated by this mode. This will cause Step 6 to fail because it
> relies on the Step 1 snapshot, which may contain tags from other projects, and
> it will not find these tags in the destination if they were not replicated.

## High-Level Approach

The script performs migration in 6 steps:

1.  **Save Policy Tag Bindings:** Snapshots policy tag bindings in the source
    region (e.g., `us`). This step identifies which table columns in the source
    dataset are protected by which policy tags. This snapshot is essential for
    Steps 2, 3, 4, and 6.
2.  **Replicate Taxonomy Tree:** Copies taxonomy structures from the source
    region (`us`) to the destination region (e.g., `us-east4`). Because Policy
    Tags are regional resources organized in taxonomies, the taxonomies
    containing the tags identified in Step 1 must be recreated in the
    destination region before they can be used there. During replication,
    taxonomies are replicated with `- <destination_region>` appended to their
    display name; for example, a source taxonomy `My Taxonomy` is replicated as
    `My Taxonomy - us-east4` in destination `us-east4`. **If a taxonomy named
    `My Taxonomy - us-east4` already exists in `us-east4`, its import will be
    skipped, an error will be logged, and the script will continue**, as it does
    not handle merging or updating pre-existing taxonomies. If Step 2 fails for
    any other reason, please do NOT proceed to future steps and reach out to
    Google instead.
3.  **Replicate Policy Tag IAM Policies:** Copies IAM policies (e.g.,
    `roles/datacatalog.fineGrainedReader`) from source policy tags (`us`) to the
    corresponding newly created destination policy tags (`us-east4`). This
    ensures that users and groups who have access to column data in the source
    region retain the column access in the destination region.
4.  **Replicate Data Policies:** Copies Data Policies (e.g., data masking rules)
    from the source region (`us`) to the destination region (`us-east4`),
    attaching them to the corresponding destination policy tags. This ensures
    that masking rules are active and enforced correctly in the destination
    region. When the Data Policy uses a Custom Masking Routine as the masking
    rule, the Custom Masking Routine will first be replicated to the destination
    region in a new dedicated dataset (specified via
    `--destination_custom_masking_routine_dataset` argument). Once the Custom
    Masking Routine is replicated, the Data Policy using that routine will be
    replicated.
5.  **Promote Replica to Primary:** Switches the dataset replica in the
    destination region (`us-east4`) to become the new writable primary replica.
    This is a standard step in a cross-region failover process, making the
    destination dataset the new source of truth. **IMPORTANT**: Step 5 includes
    a 2-minute pause after completion to allow the primary replica switch to
    fully propagate before Step 6 can be run.
6.  **Update Table Schemas:** Updates table schemas in the new primary dataset
    (`us-east4`) to point to the correct policy tags created in the destination
    region in Step 2. When a dataset replica is created, policy tag references
    in its schema become invalid (`taxonomy/0`). This step uses the snapshot
    from Step 1 to map columns back to the correct, newly created policy tags in
    `us-east4`, restoring fine-grained access control. Optionally, a backup of
    the original schema can be saved using `--table_schema_backup_dir` before
    updates are applied.

**Please review these steps and the detailed descriptions below. If you have any
concerns or questions about this approach, please stop and reach out to your
Google contact before proceeding.**

## Prerequisites

1.  Python 3.9+ installed.
2.  Google Cloud SDK (`gcloud`) installed and authenticated:
    *   Run `gcloud auth login`
3.  Required Python libraries installed. We recommend using a virtual
    environment:

    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install google-cloud-bigquery google-cloud-datacatalog google-cloud-bigquery-datapolicies google-api-python-client
    ```

4.  The user or service account running the script needs sufficient IAM
    permissions in the project. The following roles are recommended:

    *   BigQuery Admin (`roles/bigquery.admin`)
    *   Data Catalog Admin (`roles/datacatalog.admin`)
    *   Data Catalog Policy Tag Admin (`roles/datacatalog.categoryAdmin`)
    *   BigQuery Data Policy Admin (`roles/bigquerydatapolicy.admin`)
    *   Browser (`roles/browser`) or any role with
        `resourcemanager.projects.get` permission. This is required by the
        script to look up project numbers via the Cloud Resource Manager API,
        ensuring consistent resource name handling.

    **Note:** If using snapshot-based replication (default), and your dataset
    contains tables referencing policy tags from taxonomies in **other
    projects**, the user or service account running the script must have
    equivalent permissions in those projects as well to read/replicate those
    taxonomies and policies.

## Usage

Activate the virtual environment and run the script, specifying the step(s) you
wish to perform. It is recommended to run one step at a time.

```bash
source .venv/bin/activate
python migrate_policy_tags.py --project_id <YOUR_PROJECT_ID> [options] --step<N>
```

### Global Options

*   `--project_id <PROJECT_ID>`: **(Required)** Your Google Cloud project ID.
*   `--source_region <REGION>`: The source region (e.g., `us`). Required for
    steps 1-4, 6.
*   `--destination_region <REGION>`: The destination region (e.g., `us-east4`).
    Required for steps 2-6.
*   `--dataset <DATASET_ID>`: Dataset ID to process. Required for step 5, and
    steps 1 and 6 if --all_datasets is not used.
*   `--policy_tag_bindings_snapshot_timestamp <YYYYMMDDHHMMSS>`: Timestamp of a
    previous Step 1 run to use for steps 2, 3, 4, or 6. If running Step 1, a new
    timestamp is generated and used. This flag is ignored for steps 2, 3, and 4
    if `--replicate_all_taxonomies` is used.
*   `--replicate_all_taxonomies`: If provided, steps 2, 3, and 4 will replicate
    ALL taxonomies, policy tags, and data policies from the source project and
    region, instead of only those referenced in a Step 1 snapshot.
*   `--all_datasets`: If provided for Step 1, snapshots policy tag bindings for
    all datasets in `project_id` and `source_region`, instead of a single
    dataset. If provided for Step 6, updates schemas for all tables in the
    policy tag bindings snapshot. For Steps 1 and 6, either `--dataset` or
    `--all_datasets` must be provided.
*   `--policy_tag_bindings_dataset <DATASET>`: Dataset for policy tag bindings
    snapshot table, in `dataset_id` or `project_id.dataset_id` format. If
    provided, this dataset must already exist. If not provided,
    `policy_tag_bindings_dataset` in the source region project will be used, and
    created if it doesn't exist.
*   `--destination_custom_masking_routine_dataset <DATASET_ID>`: Dataset in
    destination region for storing replicated custom masking routines. Required
    for Step 4.
*   `--table_schema_backup_dir <PATH>`: If provided for Step 1, saves a backup
    of each table's schema found in the snapshot to this directory. Required for
    Step 1.
*   `--skip_confirmation_step6`: If provided, skip manual confirmation before
    applying schema changes in Step 6.
*   `--log_file <PATH>`: Path to log file (defaults to
    `migrate_policy_tags_<TIMESTAMP>.log`).
*   `--log_level <LEVEL>`: Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`).
    Default: `INFO`.

## Testing Recommendation

It is **HIGHLY RECOMMENDED** to test this script on a non-production dataset
first. Please verify that policy tags, data policies, and IAM bindings are
correctly migrated for the test dataset before running this script on production
datasets.

--------------------------------------------------------------------------------

## Detailed Steps

### Step 1: Save Policy Tag Bindings

*   **Purpose:** Before dataset promotion, this step snapshots the current
    bindings between table columns and policy tags in the source region tables
    for the specified dataset (if `--dataset` is used) or for all datasets in
    the project and region (if `--all_datasets` is used). This snapshot is saved
    to a BigQuery table named `policy_tag_bindings_<TIMESTAMP>` in the source
    region. By default, this table is created in a dataset named
    `policy_tag_bindings_dataset`. You can specify a different dataset (or
    project and dataset) using the `--policy_tag_bindings_dataset` flag; if this
    flag is used, the dataset must already exist. This snapshot is used by later
    steps to recreate bindings. If `--table_schema_backup_dir` is provided, this
    step will also save a JSON backup of the schema for each table that has
    policy tag bindings in the snapshot. The `--table_schema_backup_dir` flag is
    required for this step.
*   **Idempotency:** No. Rerunning Step 1 will attempt to create a new snapshot
    table.
*   **Usage:**

    ```bash
    # Snapshot bindings for a single dataset and backup schemas
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --dataset <DATASET_ID> \
      --policy_tag_bindings_dataset my_bindings_dataset \
      --table_schema_backup_dir ./table_schema_backups --step1

    # Snapshot bindings for all datasets in the project and region
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --all_datasets \
      --policy_tag_bindings_dataset my_bindings_dataset \
      --table_schema_backup_dir ./table_schema_backups --step1
    ```

*   **Required Permissions:**

    *   `bigquery.jobs.create`
    *   `bigquery.tables.list`
    *   `bigquery.datasets.create`
    *   `bigquery.datasets.get`
    *   `bigquery.tables.create`

### Step 2: Replicate Taxonomy Tree

*   **Purpose:** Copies taxonomy structures from the source to the destination
    region.
    *   If `--replicate_all_taxonomies` is **provided**, all taxonomies in the
        source project and region are replicated.
    *   If `--replicate_all_taxonomies` is **not provided**, only taxonomies
        referenced in the Step 1 snapshot (specified by
        `--policy_tag_bindings_snapshot_timestamp`) are replicated.
*   **Process:** Taxonomies are exported from the source and imported into the
    destination. To avoid name collisions, the script appends the destination
    region name to the display name of each taxonomy being created in the
    destination (e.g., `My Taxonomy` becomes `My Taxonomy - us-east4`). If a
    taxonomy was already replicated in a previous run, an "Already Exists" error
    will be logged, and the script will skip importing that specific taxonomy.
    For example, you can expect this when replicating `dataset2` if `My
    Taxonomy`, which `dataset2` uses, has already been replicated when you
    processed `dataset1` earlier.
*   **Idempotency:** Partially. If run again, taxonomies that already exist in
    the destination with the correct suffixed name will be skipped, and an
    `AlreadyExists` message will be logged. The script will not halt on
    `AlreadyExists` errors but will proceed with other taxonomies or projects.
*   **Usage:**

    ```bash
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 \
      --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> \
      --policy_tag_bindings_dataset my_bindings_dataset --step2

    # Or, to replicate all taxonomies in us-east4 without a snapshot
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 \
      --replicate_all_taxonomies --step2
    ```

*   **Required Permissions:**

    *   `bigquery.jobs.create`
    *   `bigquery.tables.getData`
    *   `datacatalog.taxonomies.list`
    *   `datacatalog.taxonomies.get`
    *   `datacatalog.policyTags.get`
    *   `datacatalog.taxonomies.import`
    *   `datacatalog.taxonomies.create`

### Step 3: Replicate Policy Tag IAM Policies

*   **Purpose:** Copies IAM policies (e.g.,
    `roles/datacatalog.fineGrainedReader`) from source policy tags to their
    corresponding policy tags in destination taxonomies created in Step 2.
    *   If `--replicate_all_taxonomies` is **provided**, IAM policies for tags
        in all taxonomies in the source project and region are replicated.
    *   If `--replicate_all_taxonomies` is **not provided**, only IAM policies
        for tags in taxonomies referenced in the Step 1 snapshot are replicated.
*   **Process:** The script identifies corresponding policy tags between source
    and destination taxonomies by matching their display names.
*   **Idempotency:** Yes. Rerunning this step will overwrite IAM policies on
    destination tags with the current policies from source tags.
*   **Assumption:** Policy tag display names are unique within a given taxonomy.
*   **Usage:**

    ```bash
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 \
      --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> \
      --policy_tag_bindings_dataset my_bindings_dataset --step3

    # Or, to replicate IAM policies for all taxonomies in us-east4
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 \
      --replicate_all_taxonomies --step3
    ```

*   **Required Permissions:**

    *   `bigquery.jobs.create`
    *   `bigquery.tables.getData`
    *   `datacatalog.taxonomies.list`
    *   `datacatalog.taxonomies.get`
    *   `datacatalog.policyTags.list`
    *   `datacatalog.policyTags.getIamPolicy`
    *   `datacatalog.policyTags.setIamPolicy`

### Step 4: Replicate Data Policies

*   **Purpose:** Copies Data Policies (data masking rules) from the source
    region to the destination region, linking them to the corresponding
    destination policy tags. It also replicates IAM policies set on the Data
    Policies themselves.
    *   If `--replicate_all_taxonomies` is **provided**, Data Policies linked to
        any policy tag in any taxonomy in the source region are replicated.
    *   If `--replicate_all_taxonomies` is **not provided**, only Data Policies
        linked to policy tags referenced in the Step 1 snapshot are replicated.
*   **Process:** The script lists all data policies in the source region, finds
    those linked to policy tags belonging to the taxonomies being replicated,
    and recreates them in the destination region, attached to the corresponding
    destination policy tag.
*   **Idempotency:** Yes. If a data policy already exists in the destination,
    creation is skipped, and the script proceeds to sync its IAM policy.
*   **Limitation:** If a data policy uses a custom masking routine (UDF), this
    step will attempt to replicate the routine to the dataset specified by
    `--destination_custom_masking_routine_dataset`. Ensure this dataset exists
    or can be created in the destination region, and that the routine's SQL body
    is compatible with BigQuery in the destination region.
*   **Usage:**

    ```bash
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 \
      --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> \
      --policy_tag_bindings_dataset my_bindings_dataset \
      --destination_custom_masking_routine_dataset my_routines_us_east4 --step4

    # Or, to replicate data policies for all taxonomies in us-east4
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 \
      --replicate_all_taxonomies \
      --destination_custom_masking_routine_dataset my_routines_us_east4 --step4
    ```

*   **Required Permissions:**

    *   `bigquery.jobs.create`
    *   `bigquery.tables.getData`
    *   `datacatalog.taxonomies.list`
    *   `datacatalog.taxonomies.get`
    *   `datacatalog.policyTags.list`
    *   `bigquerydatapolicy.dataPolicies.list`
    *   `bigquerydatapolicy.dataPolicies.create`
    *   `bigquerydatapolicy.dataPolicies.getIamPolicy`
    *   `bigquerydatapolicy.dataPolicies.setIamPolicy`
    *   If custom routines are used: `bigquery.routines.get`,
        `bigquery.datasets.create`, `bigquery.datasets.get`,
        `bigquery.routines.create`.

### Step 5: Promote Replica to Primary

*   **Purpose:** Promotes the BigQuery dataset replica in the destination region
    to become the new writable primary replica. This step should align with your
    organization's dataset failover/promotion process.
*   **Idempotency:** Yes. Rerunning promotion on an already-promoted dataset has
    no effect.
*   **CRITICAL WARNING:** After this step, columns with policy tags **WILL
    BECOME INACCESSIBLE** in BOTH the source region and the destination region
    until Step 6 is run. This is because the schema in the newly promoted
    primary region still points to non-existent `taxonomy/0` policy tags. **It
    is HIGHLY RECOMMENDED to run Step 6 immediately after Step 5.**
    **IMPORTANT**: Step 5 includes a 2-minute pause after completion to allow
    the primary replica switch to fully propagate before Step 6 can be run.
*   **Usage:**

    ```bash
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 --dataset <DATASET_ID> --step5
    ```

*   **Required Permissions:**

    *   `bigquery.datasets.update`

### Step 6: Update Table Schemas

*   **Purpose:** Updates table schemas in the new primary (the destination
    region) to restore access to protected columns. It replaces invalid policy
    tag references (`taxonomy/0`) with references to the correct policy tags
    created in the destination region during Step 2. This step ONLY fixes the
    access issue in the new primary (destination region, e.g., `us-east4`). It
    **DOES NOT** fix access issues in the new secondary region (i.e., the former
    source region, e.g., `us`), where columns with policy tags will remain
    inaccessible.
*   **Process:** The script reads the snapshot from Step 1 and the current
    schema of each table found in the snapshot (or filtered by `--dataset`). If
    a column schema contains a `taxonomy/0` policy tag reference, the script
    uses the snapshot to find which policy tag *should* be applied, finds the
    corresponding policy tag in the destination region taxonomy, and updates the
    table schema to point to it. Policy tags are matched between source and
    destination using their full hierarchical path (e.g., `ParentTag.ChildTag`).
*   **Idempotency:** Yes. The script only attempts to update columns that
    reference `taxonomy/0`. If tags have already been corrected by a previous
    run, this step will skip updates for that table.
*   **Assumption:** The policy tag hierarchy (parent/child relationships) is
    identical between the source and destination taxonomies.
*   **Usage:**

    ```bash
    # Update schemas for a single dataset
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 --dataset <DATASET_ID> \
      --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> \
      --policy_tag_bindings_dataset my_bindings_dataset --step6

    # Update schemas for all tables in the snapshot
    python migrate_policy_tags.py --project_id <PROJECT_ID> \
      --source_region us --destination_region us-east4 --all_datasets \
      --policy_tag_bindings_snapshot_timestamp <TIMESTAMP> \
      --policy_tag_bindings_dataset my_bindings_dataset --step6
    ```

*   **Required Permissions:**

    *   `bigquery.jobs.create`
    *   `bigquery.tables.getData`
    *   `datacatalog.taxonomies.list`
    *   `datacatalog.taxonomies.get`
    *   `datacatalog.policyTags.list`
    *   `bigquery.tables.get`
    *   `bigquery.tables.update`
    *   `bigquery.tables.setCategory`
