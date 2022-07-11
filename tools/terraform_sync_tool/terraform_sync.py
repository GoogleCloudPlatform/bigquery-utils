import json
import io
import argparse
import re
from google.cloud import bigquery
import google.auth


# Fetch schemas for drifted tables form BQ
def get_schemas_from_BQ(tables_of_interest, client):
    table_schemas = []
    for k in tables_of_interest:
        table_id = tables_of_interest.get(k).get('table_id')
        table = client.get_table(table_id)
        file = io.StringIO("")
        client.schema_to_json(table.schema, file)
        # append schema as a dict where key=table_id, value=table_schema
        schema_bq = dict({table_id: json.loads(file.getvalue())}) 
        table_schemas.append(schema_bq)
    return table_schemas

# Identify tables with drifts and add return tables_of_interest 
def get_drifted_tables(json_file):
    tables_of_interest = {} # store events(dict) of interest where key=resource_name+resource_key as identifier and value=resource_id                                                                                                    
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
                # Use condensed resource_name+resource_key as key of new dict
                resource_condensed = change_resource.get('resource_name')+change_resource.get('resource_key')
                # Use resource infomation from resource_drift event in value of new dict
                resource_from_drift = {'identifier':
                    {
                    'resource_name':change_resource.get('resource_name'),
                    'resource_key':change_resource.get('resource_key')
                    }}
                # Add to tables_of_interest
                tables_of_interest[resource_condensed] =  resource_from_drift
            # When refresh_complete event detected, add id_value and convert it to fully-qualified table_id, and add table_id to dict
            if type == 'refresh_complete' and hook and hook.get('resource') and hook.get('resource').get('resource_type') == 'google_bigquery_table':
                hook_resource = hook.get('resource')
                # Use condensed resource_name+resource_key as the key to check if it exists in tables_of_interest
                resource_condensed = hook_resource.get('resource_name') + hook_resource.get('resource_key')
                # Usee resource information from refresh_complete event as identifier
                resource_from_hook = {
                        'resource_name':hook_resource.get('resource_name'),
                        'resource_key':hook_resource.get('resource_key')
                        }
                # When the resource_condensed exists and resource_from_hook matched with identifier in dict
                # add id_value and convert it to fully-qualified table_id, and add table_id to dict
                if resource_condensed in tables_of_interest and resource_from_hook == tables_of_interest.get(resource_condensed).get('identifier'):
                    drifted_table_name = hook.get('id_value')
                    tables_of_interest[resource_condensed]['id_value'] = drifted_table_name

                    x = re.findall(r'projects/(.*)/datasets/(.*)/tables/(.*)', drifted_table_name)
                    table_id = '.'.join(list(x[0]))
                    tables_of_interest[resource_condensed]['table_id'] = table_id
        return tables_of_interest

def main():
    # Provide arguments for JSON filename
    parser = argparse.ArgumentParser(description='user-provided arguments')
    parser.add_argument('filename')
    args = parser.parse_args()

    # Parse json file to identify drifted tables
    tables_of_interest = get_drifted_tables(args.filename)   
        
    # Fail the build and Fetch latest schemas if drifts are detected    
    if tables_of_interest:
        #obtain credentials
        credentials, project_id = google.auth.default()
        # Fetch latest schemas for drifted tables from BQ
        client = bigquery.Client(project_id)
        drifted_table_schemas = get_schemas_from_BQ(tables_of_interest, client)
        # Drifts detected, throw exceptions
        raise Exception("Drifts are detected in these tables, please update your terraform schema files with the following updated table schemas. ", drifted_table_schemas)

if __name__ == "__main__":
    main()