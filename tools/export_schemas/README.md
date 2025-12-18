# BigQuery Schema Export Tool

This tool exports the DDL (Data Definition Language) for all tables in a BigQuery project's region to local SQL files, organized by dataset.


## Use Cases

- **AI Context Provisioning**: Provide a snapshot of your current DDLs to AI models for high-fidelity schema context.
- **Version Control**: Quickly dump all table schemas to Git for version tracking.
- **Migration**: Export schemas to specific folders to assist in migrating datasets between projects or regions.
- **Backup**: Create a snapshot of your current DDLs for disaster recovery or audit purposes.
- **Local Development**: Analyze table structures offline without querying BigQuery repeatedly.
- **CI/CD**: Use as a step in a CI/CD pipeline to export schemas for testing or validation.

## Prerequisites

- Google Cloud SDK: The bq command-line tool must be installed and authenticated.
- Python 3.x: Installed on your local machine or Cloud Shell.
- IAM Permissions:
    - roles/bigquery.jobUser (To execute the query job)
    - roles/bigquery.metadataViewer (To access the INFORMATION_SCHEMA.TABLES view)

## Usage

```bash
python3 export_schemas.py --project_id <YOUR_PROJECT_ID> [--region <REGION>] [--output_dir <OUTPUT_DIR>]
```

### Arguments

- `--project_id`: (Required) The Google Cloud Project ID.
- `--region`: (Optional) The BigQuery region to query. Defaults to `us`.
- `--output_dir`: (Optional) The directory to save the exported schemas. Defaults to `bq_schemas`.

### Example

```bash
python3 export_schemas.py --project_id my-data-project --region us-east1
```

### Output

The tool will create a folder structure like this:

```
bq_schemas/
  ├── dataset_a/
  │   ├── table1.sql
  │   └── table2.sql
  └── dataset_b/
      └── table3.sql
```

It will also generate a zip file `bq_schema_export.zip` containing all the exported schemas.
