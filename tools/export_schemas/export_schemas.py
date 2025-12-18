# Copyright 2025 Google LLC
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

import subprocess
import json
import os
import shutil
import argparse
import sys

def parse_args():
    parser = argparse.ArgumentParser(description="Export BigQuery table schemas and DDLs to local files.")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us", help="BigQuery Region (default: us-central1)")
    parser.add_argument("--output_dir", default="bq_schemas", help="Output directory for exported schemas (default: bq_schemas)")
    return parser.parse_args()

def main():
    args = parse_args()
    
    project_id = args.project_id
    region = args.region
    output_dir = args.output_dir
    
    # Construct region scope for INFORMATION_SCHEMA
    if region.lower().startswith("region-"):
          region_scope = region
    else:
          region_scope = f"region-{region}"

    # 1. Setup clean directory
    if os.path.exists(output_dir):
        print(f"Cleaning existing directory: {output_dir}")
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    print(f"--- Starting Bulk Export for {project_id} ({region_scope}) ---")

    # 2. Run the query using BQ CLI and get JSON output (Safe for DDL parsing)
    query = f"""
    SELECT table_schema, table_name, ddl
    FROM `{project_id}.{region_scope}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'BASE TABLE'
    """
    
    print("Querying BigQuery metadata...")
    
    cmd = [
        "bq", "query", 
        "--use_legacy_sql=false", 
        "--format=json", 
        "--max_rows=10000", 
        f"--project_id={project_id}",
        query
    ]

    try:
        # Run command and capture output
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        tables = json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        print("\nError running BigQuery command:")
        print(e.stderr)
        return
    except json.JSONDecodeError:
        print("\nError: Query returned no data or invalid JSON.")
        print("Check if your region is correct or if you have permissions.")
        return

    if not tables:
        print("No tables found. Check your project ID and region.")
        return

    print(f"Found {len(tables)} tables. Writing .sql files...")

    # 3. Write files
    for row in tables:
        dataset = row['table_schema']
        table = row['table_name']
        ddl = row['ddl']

        # Create dataset folder if it doesn't exist
        ds_path = os.path.join(output_dir, dataset)
        os.makedirs(ds_path, exist_ok=True)

        # Write the DDL
        with open(os.path.join(ds_path, f"{table}.sql"), "w") as f:
            f.write(ddl)

    # 4. Create Zip
    print("Zipping files...")
    shutil.make_archive("bq_schema_export", 'zip', output_dir)

    print("\n----SUCCESS----!")
    print(f"File created: {os.path.abspath('bq_schema_export.zip')}")
    print(f"Schemas exported to: {os.path.abspath(output_dir)}")

if __name__ == "__main__":
    main()
