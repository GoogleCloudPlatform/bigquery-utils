import base64
import json
from google.cloud import bigquery
 
def trigger_table_1(event, context):
   """Triggered from a message on a Cloud Pub/Sub topic.
   Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
   """
   msg = base64.b64decode(event['data']).decode('utf-8')
   payload = json.loads(msg)
   bq_object = payload['protoPayload']['serviceData']['jobCompletedEvent']['job']['jobConfiguration']['query']['destinationTable']
 
   client = bigquery.Client()
   query = f"call constraints_staging.check_constraints(‘{bq_object['tableId']}’, ‘{bq_object['datasetId']}’)"
   query_job = client.query(query)
   print(f"Running: {query}; BigQuery JobID: {query_job.job_id}")

