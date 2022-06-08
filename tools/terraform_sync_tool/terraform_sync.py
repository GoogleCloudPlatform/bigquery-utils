import json
import io
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

# Convert table resource from terraform log output to 
# table_id format:[gcp_project_id].[dataset_id].[table_id]
def convert_to_table_id(input):
    table_id = ""
    for s in input.rsplit("/"):
        if(s != "projects" and s != "datasets" and s != "tables"):
            table_id += s+"."
    return table_id[:len(table_id)-1]

# Opening JSON file
f = open('./state.json', 'r')
data = f.read()
data = data.split("}\n")
data = [d.strip() + "}" for d in data]
data = list(filter(("}").__ne__, data))
data = [json.loads(d) for d in data]
drifted_tables = []
# find table with drifts and add to drifted_tables list
for line in data: 
    if line['type'] == 'resource_drift' and line['change']['resource']['resource_type'] == 'google_bigquery_table':
        table_id = line['change']['resource']['resource_key']
        drifted_tables.append(table_id) 

# Convert drifted_tables format to table_ids
if len(drifted_tables) > 0: 
    # iterate through drifted_tables list
    i = 0 
    for line in data:
        if line['type'] == 'refresh_complete':
            resource_table = line['hook']['id_value']
            if(resource_table[len(resource_table) - len(drifted_tables[i]):] == drifted_tables[i]):
                drifted_tables[i] = convert_to_table_id(resource_table)
                i += 1
            if(i == len(drifted_tables)):
                break
    # Fetch schemas for drifted tables from BQ
    drifted_table_schemas = get_schemas_from_BQ(drifted_tables)
    # Drifts detected, throw exceptions
    raise Exception("Drifts are detected in these tables, please update your terraform schema files with the following updated table schemas. ", drifted_table_schemas)


# Closing file
f.close()