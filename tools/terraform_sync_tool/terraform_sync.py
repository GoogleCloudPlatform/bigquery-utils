import json
import io
import argparse
from google.cloud import bigquery

# Fetch schemas for drifted tables form BQ
def get_schemas_from_BQ(drifted_tables):
    table_schemas = []
    client = bigquery.Client()
    for table_id in drifted_tables:
        table = client.get_table(table_id)
        file = io.StringIO("")
        client.schema_to_json(table.schema, file)
        # append schema as a dict where key=table_id, value=table_schema
        schema_bq = dict({table_id: json.loads(file.getvalue())}) 
        table_schemas.append(schema_bq)
    return table_schemas

# Identify tables with drifts and add return drifted_tables list 
def get_drifted_tables(json_file):
    # Opening JSON file
    with open(json_file) as file:
        lines = file.readlines()
        drifted_tables = []
        drifted_table = {}                                                                                                    
        for line in reversed(lines):                                                                    
            json_line = json.loads(line)
            type = json_line.get('type')
            # Scan the json lines to detect drifts
            if type:
                if type == 'resource_drift' and json_line.get('change').get('resource').get('resource_type') == 'google_bigquery_table':
                    drifted_table = {
                        'resource_name':json_line.get('change').get('resource').get('resource_name'),
                        'resource_key':json_line.get('change').get('resource').get('resource_key')
                        }
                # Trace the origins for drifted_table and convert table_name format to fully-qualified table_id
                if json_line.get('type') == 'refresh_complete' and  json_line.get('hook').get('resource').get('resource_type') == 'google_bigquery_table':
                    event_table = {
                        'resource_name':json_line.get('hook').get('resource').get('resource_name'),
                        'resource_key':json_line.get('hook').get('resource').get('resource_key')
                        }
                    # Match drifted_table with event_table using resource_name and resource_key as identifiers
                    if(drifted_table == event_table):
                        # Retrive the drifted_table_name and convert it into 
                        # table_id format:[gcp_project_id].[dataset_id].[table_id]
                        drifted_table_name = json_line.get('hook').get('id_value')
                        table_id = ""
                        for s in drifted_table_name.rsplit("/"):
                            if(s != "projects" and s != "datasets" and s != "tables"):
                                table_id += s+"."
                        drifted_tables.append(table_id[:len(table_id)-1]) 
    return drifted_tables

def main():
    parser = argparse.ArgumentParser(description='user-provided arguments')
    parser.add_argument('filename')
    args = parser.parse_args()

    # Parse json file to identify drifted tables
    drifted_tables = get_drifted_tables(args.filename)                   
        
    if drifted_tables:
        # Fetch latest schemas for drifted tables from BQ
        drifted_table_schemas = get_schemas_from_BQ(drifted_tables)
        # Drifts detected, throw exceptions
        raise Exception("Drifts are detected in these tables, please update your terraform schema files with the following updated table schemas. ", drifted_table_schemas)

if __name__ == "__main__":
    main()