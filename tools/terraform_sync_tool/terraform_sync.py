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

def main():
    parser = argparse.ArgumentParser(description='user-provided arguments')
    parser.add_argument('filename')
    args = parser.parse_args()

    # Opening JSON file
    with open(args.filename) as file:
        lines = file.readlines()
        drifted_tables = []
        drifted_table = ''                                                                                                    
        for line in reversed(lines):                                                                    
            json_line = json.loads(line)
            type = json_line.get('type')
            # Scan the json lines to detect drifts
            if type:
                if type == 'resource_drift' and json_line.get('change').get('resource').get('resource_type') == 'google_bigquery_table':
                    table_name = json_line.get('change').get('resource').get('resource_key')
                    drifted_table = table_name
                if json_line.get('type') == 'refresh_complete':
                    resource_table = json_line.get('hook').get('id_value')
                    if(resource_table[len(resource_table) - len(drifted_table):] == drifted_table):
                        # Convert table resource from terraform log output to 
                        # table_id format:[gcp_project_id].[dataset_id].[table_id]
                        table_id = ""
                        for s in resource_table.rsplit("/"):
                            if(s != "projects" and s != "datasets" and s != "tables"):
                                table_id += s+"."
                        drifted_tables.append(table_id[:len(table_id)-1])                        
        
        if drifted_tables:
            # Fetch latest schemas for drifted tables from BQ
            drifted_table_schemas = get_schemas_from_BQ(drifted_tables)
            # Drifts detected, throw exceptions
            raise Exception("Drifts are detected in these tables, please update your terraform schema files with the following updated table schemas. ", drifted_table_schemas)

if __name__ == "__main__":
    main()