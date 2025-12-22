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

import os
import shutil
import argparse
import sys
import concurrent.futures
from google.cloud import bigquery

def parse_args():
    parser = argparse.ArgumentParser(description="Export BigQuery table schemas and DDLs to local files.")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--region", default="us", help="BigQuery Region (default: us")
    parser.add_argument("--output_dir", default="bq_schemas", help="Output directory for exported schemas (default: bq_schemas)")
    return parser.parse_args()



def write_ddl(table_metadata, output_dir):
    dataset = table_metadata['table_schema']
    table = table_metadata['table_name']
    ddl = table_metadata['ddl']

    # Create dataset folder if it doesn't exist
    ds_path = os.path.join(output_dir, dataset)
    os.makedirs(ds_path, exist_ok=True)

    # Write the DDL
    with open(os.path.join(ds_path, f"{table}.sql"), "w") as sql_file:
        sql_file.write(ddl)

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

    # 2. Run the query using BigQuery Client Library
    query = f"""
    SELECT table_schema, table_name, ddl
    FROM `{project_id}.{region_scope}.INFORMATION_SCHEMA.TABLES`
    WHERE table_type = 'BASE TABLE'
    """
    
    print("Querying BigQuery metadata...")
    
    try:
        client = bigquery.Client(project=project_id, location=region)
        query_job = client.query(query)
        tables = [dict(table_metadata) for table_metadata in query_job]
    except ImportError:
        print("\nError: google-cloud-bigquery module not found.")
        print("Please install using: pip install google-cloud-bigquery or requirements.txt")
        return
    except Exception as query_error:
        print("\nError running BigQuery query:")
        print(query_error)
        return

    if not tables:
        print("No tables found. Check your project ID and region.")
        return

    print(f"Found {len(tables)} tables. Writing {len(tables)} .sql files...")

    # 3. Write files in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_table = {
            executor.submit(write_ddl, table_metadata, output_dir): table_metadata 
            for table_metadata in tables
        }
        for future in concurrent.futures.as_completed(future_to_table):
            table_metadata = future_to_table[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Table {table_metadata.get('table_name')} generated an exception: {exc}")

    # 4. Create Zip
    print("Zipping files...")
    shutil.make_archive("bq_schema_export", 'zip', output_dir)

    print("\n----SUCCESS----!")
    print(f"File created: {os.path.abspath('bq_schema_export.zip')}")
    print(f"Schemas exported to: {os.path.abspath(output_dir)}")

if __name__ == "__main__":
    main()
