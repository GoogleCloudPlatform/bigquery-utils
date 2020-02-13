import sys
import json
from jinja2 import Template

def main(argv):
  if len(argv) < 2:
    print('usage: bq_json2ddl.py <schema.json> <outputfile>')
    sys.exit()
  output_file = argv[1]

  json_file = open(argv[0], 'r')
  schema = json.load(json_file)
  json_file.close()

  ddl_template = Template("""
  CREATE TABLE `{{ schema.tableReference.projectId }}`.{{ schema.tableReference.datasetId }}.{{ schema.tableReference.tableId }}
  (
  {%- for field in schema.schema.fields recursive %}
    {{ '{} '.format(field.name) }}   
    
    {%- if field.mode == 'REPEATED' -%}
    ARRAY<
    {%- endif %}

    {%- if field.type == 'RECORD' -%}
    STRUCT< {% filter indent(2) %} {{ loop(field.fields) }} {% endfilter %}
    >
    {%- elif field.type == 'FLOAT' -%}
    FLOAT64  
    {%- elif field.type == 'INTEGER' -%}
    INT64  
    {%- else -%}
    {{ field.type }}
    {%- endif %}
    
    {%- if field.mode == 'REPEATED' -%}
    >
    {%- elif field.mode == 'REQUIRED' -%}
    {{ ' NOT NULL' }}
    {%- endif %}
    
    {%- if field.description -%}
    {{ ' OPTIONS(description="{}")'.format(field.description) }}
    {%- endif %}
    {%- if loop.nextitem is defined %}, {% endif %}
  {%- endfor %}
  )

  {%- if partitioning.type == "timePartitioning" %} 
  PARTITION BY {{ partitioning.desc }}
  {%- elif partitioning.type == "rangePartitioning" %} 
  PARTITION BY RANGE_BUCKET({{ partitioning.desc.field }}, GENERATE_ARRAY({{ partitioning.desc.range.start }}, {{ partitioning.desc.range.end }}, {{ partitioning.desc.range.interval }}))
  {%- endif %}

  {%- if schema.clustering %} 
  CLUSTER BY 
    {%- for col in schema.clustering.fields %}
    {{ col }} {%- if loop.nextitem is defined %}, {%- endif %}
    {%- endfor %}
  {%- endif %}

  {%- if options %} 
  OPTIONS(
  {%- for oname, ovalue in options.items() %}
    {%- if oname == "labels" %} 
    {{ oname }} = [
      {%- for lname, lvalue in ovalue.items() %}
      ( "{{ lname }}", "{{ lvalue }}" ) {%- if loop.nextitem is defined %}, {%- endif %}
      {%- endfor %}]
    {%- else %}
    {{ oname }} = {{ ovalue }}
    {%- endif %}
    {%- if loop.nextitem is defined %}, {%- endif %}
  {%- endfor %}
  )
  {%- endif %}
  """)

  obj_partition = {}
  obj_options = {}

  # Parse PARTITION section:
  if "rangePartitioning" in schema:
      obj_partition["type"] = "rangePartitioning"
      obj_partition["desc"] = schema["rangePartitioning"]
  elif "timePartitioning" in schema:
      obj_partition["type"] = "timePartitioning"
      if "field" in schema["timePartitioning"]:
          col_name = schema["timePartitioning"]["field"]
          if 'TIMESTAMP' in [field["type"] for field in schema["schema"]["fields"] if field["name"] == col_name]:
              obj_partition["desc"] = "DATE({})".format(col_name)
          else:
              obj_partition["desc"] = col_name
      else:
          obj_partition["desc"] = "_PARTITIONDATE"

  # Parse OPTIONS section:
  if "expirationTime" in schema:
      obj_options["expiration_timestamp"] = 'TIMESTAMP_MILLIS({})'.format(schema["expirationTime"])
  if "encryptionConfiguration" in schema:
      obj_options["kms_key_name"] = '"{}"'.format(schema["encryptionConfiguration"]["kmsKeyName"]) 
  if "friendlyName" in schema:
      obj_options["friendly_name"] = '"{}"'.format(schema["friendlyName"])  
  if "description" in schema:
      obj_options["description"] = '"""{}"""'.format(schema["description"])
  if "labels" in schema:
      obj_options["labels"] = schema["labels"]
  if "requirePartitionFilter" in schema:
      obj_options["require_partition_filter"] = schema["requirePartitionFilter"]
  if obj_partition:
      if "expirationMs" in obj_partition["desc"]:
          obj_options["partition_expiration_days"] = int(obj_partition["desc"]["expirationMs"]) / 86400000

  ddl = ddl_template.render(schema = schema, 
                            partitioning = obj_partition,
                            options = obj_options)
  # print(ddl)
  with open(output_file, 'w') as fw:
    fw.write(ddl)

if __name__ == '__main__':
    main(sys.argv[1:])


