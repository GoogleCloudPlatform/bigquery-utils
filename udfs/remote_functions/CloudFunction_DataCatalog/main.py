from __future__ import print_function
import base64
import json
import os
from google.cloud import datacatalog_v1
import time
from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf import timestamp_pb2


def catalog_handler(request):
    request_json = request.get_json()
    mode = request_json['userDefinedContext']['mode']
    calls = request_json['calls']
    if mode == "upsert":
        return dc_router(calls)
    return json.dumps({"Error in Request": request_json}), 400


def dc_router(calls):
    return_value = []

    for call in calls:
        msg=call[0]
        dataset=call[1]
        table_name=call[2]
        table_rows=call[3]
        rows_changed=call[4]

        print("Update msg: " + msg)
        print("Dataset: " + dataset)
        print("Table: " + table_name)
        print("Total Rows: " + str(table_rows))        
        print("Rows Changed: " + str(rows_changed))        

        ##############
        # Insert call here
        upsert_result = upsert_tag(msg,dataset,table_name,table_rows,rows_changed)
        print("UPSERT_RESULT= " + str(upsert_result))
        ##############
        
        return_value.append(str(upsert_result))
    return_json = json.dumps({"replies": return_value})
    return return_json


def upsert_tag(msg,dataset,table_name,table_rows,rows_changed):
    tag_template_id = "<your-tag-template-id>"
    project_id = "<your-project-id>"

    dataset_id = dataset
    table_count = table_rows
    job_msg=msg
    job_rows_changed=rows_changed
    
    # Set table_id to the ID of existing table.
    table_id=table_name
    # Tag template to create.


    # [START data_catalog_quickstart]
    # For all regions available, see:
    # https://cloud.google.com/data-catalog/docs/concepts/regions
    #location = "us-central1"
    location = "us"

    tag_template = "projects/" + project_id + "/locations/" + location + "/tagTemplates/" + tag_template_id

    
    # Attach a Tag to the table.
    tag = datacatalog_v1.types.Tag()
    
    # Use Application Default Credentials to create a new
    # Data Catalog client. GOOGLE_APPLICATION_CREDENTIALS
    # environment variable must be set with the location
    # of a service account key file.
    datacatalog_client = datacatalog_v1.DataCatalogClient()

    #timestamp = Timestamp()
    
    
    # Lookup Data Catalog's Entry referring to the table.
    resource_name = (
        f"//bigquery.googleapis.com/projects/{project_id}"
        f"/datasets/{dataset_id}/tables/{table_id}"
    )

    table_entry = datacatalog_client.lookup_entry(
        request={"linked_resource": resource_name}
    )

    #print(table_entry.name)

    page_result = datacatalog_client.list_tags(parent=table_entry.name)

    try:
        counter=0
        action=""
        for response in page_result:
            if response.template == tag_template:   #Means found the tag template. Updating it
                now = time.time()
                seconds = int(now)
                nanos = int((now - seconds) * 10**9)
                timestamp = Timestamp(seconds=seconds, nanos=nanos)

                tag = response
                tag.fields["source"].string_value = job_msg
                tag.fields["num_rows"].double_value = table_count
                tag.fields["changed_rows"].double_value = job_rows_changed
                tag.fields["last_update"].timestamp_value = timestamp
                response = datacatalog_client.update_tag(tag=tag)
                print("Tag Updated")
                action="Tags Updated"
                counter=counter+1    
        print(table_entry.name)
        if counter == 0:  #Means new entry. Create a new tag
            
            now = time.time()
            seconds = int(now)
            nanos = int((now - seconds) * 10**9)
            timestamp = Timestamp(seconds=seconds, nanos=nanos)
            
            # Attach a Tag to the table.
            tag = datacatalog_v1.types.Tag()
            tag.template= tag_template
            print(tag_template)
            tag.name = "my_super_cool_tag"
            tag.fields["source"] = datacatalog_v1.types.TagField()
            tag.fields["source"].string_value = job_msg
            tag.fields["num_rows"] = datacatalog_v1.types.TagField()
            tag.fields["num_rows"].double_value = table_count
            tag.fields["changed_rows"] = datacatalog_v1.types.TagField()
            tag.fields["changed_rows"].double_value = job_rows_changed
            tag.fields["last_update"] = datacatalog_v1.types.TagField()
            tag.fields["last_update"].timestamp_value = timestamp
            tag = datacatalog_client.create_tag(parent=table_entry.name, tag=tag)
            print(f"Created tag: {tag.name}")
            action="No tags existed. Tags Created"
        return action
    except Exception as e:
        print("Exception caught: " + str(e))
        action=str(e)
        return json.dumps( { "errorMessage": action } ), 400


