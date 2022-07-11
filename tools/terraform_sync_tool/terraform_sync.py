import json
import io
import argparse
import re
from google.cloud import bigquery
import google.auth


# Fetch schemas for drifted tables form BQ
def get_schemas_from_BQ(drifted_tables, client):
    table_schemas = []
    for table_id in drifted_tables:
        table = client.get_table(table_id)
        file = io.StringIO("")
        client.schema_to_json(table.schema, file)
        # append schema as a dict where key=table_id, value=table_schema
        schema_bq = dict({table_id: json.loads(file.getvalue())}) 
        table_schemas.append(schema_bq)
    return table_schemas

# Identify tables with drifts and add return tables_of_interest 
def get_drifted_tables(json_file):
    tables_of_interest = set() # set of identifiers of interest in format resource_name+resource_key
    drifted_tables = [] # list of table IDs of drifted tables                                                                                                   
    # Opening JSON file
    with open(json_file) as file:
        lines = file.readlines()
        # Scan json lines to store events of interest
        for line in reversed(lines):                                                                    
            json_line = json.loads(line)
            type = json_line.get('type')
            change = json_line.get('change')   
            hook = json_line.get('hook') 
            # When resource_drift event detected, append new dict to tables_of_interest
            if type == 'resource_drift' and change and change.get('resource') and change.get('resource').get('resource_type') == 'google_bigquery_table':
                change_resource = change.get('resource')
                # Use condensed resource_name+resource_key as identifier
                resource_from_drift = change_resource.get('resource_name') + change_resource.get('resource_key')
                # Add to tables_of_interest
                tables_of_interest.add(resource_from_drift)
            # When refresh_complete event detected, add id_value and convert it to fully-qualified table_id, and add table_id to dict
            if type == 'refresh_complete' and hook and hook.get('resource') and hook.get('resource').get('resource_type') == 'google_bigquery_table':
                hook_resource = hook.get('resource')
                # Use condensed resource_name+resource_key to check if it exists in tables_of_interest
                resource_from_hook = hook_resource.get('resource_name') + hook_resource.get('resource_key')
                # When the resource_identifier exists in tables of interst,
                # add id_value and convert it to fully-qualified table_id, and add table_id to drifted_tables list
                if resource_from_hook in tables_of_interest:
                    drifted_table_name = hook.get('id_value')
                    # Convert id_value to qualified table ID
                    x = re.findall(r'projects/(.*)/datasets/(.*)/tables/(.*)', drifted_table_name)
                    table_id = '.'.join(list(x[0]))
                    drifted_tables.append(table_id)
    return drifted_tables

def main():
    # Provide arguments for JSON filename
    parser = argparse.ArgumentParser(description='user-provided arguments: filename of terragrunt ouput JSON file')
    parser.add_argument('filename')
    args = parser.parse_args()

    # Parse json file to identify drifted tables
    drifted_tables = get_drifted_tables(args.filename)   
        
    # Fail the build and Fetch latest schemas if drifts are detected    
    if drifted_tables:
        # Obtain credentials
        credentials, project_id = google.auth.default()
        # Fetch latest schemas for drifted tables from BQ
        client = bigquery.Client(project_id, credentials)
        drifted_table_schemas = get_schemas_from_BQ(drifted_tables, client)
        # Drifts detected, throw exceptions
        raise Exception("Drifts are detected in these tables, please update your terraform schema files with the following updated table schemas. ", drifted_table_schemas)

if __name__ == "__main__":
    main()