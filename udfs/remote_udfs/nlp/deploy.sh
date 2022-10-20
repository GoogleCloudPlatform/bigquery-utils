# Putting it all together

PROJECT=$1
LOCATION=$2
DATASET=$3

echo "Creating the connection."
bq mk --connection \
    --display_name=remote_connection \
    --connection_type=CLOUD_RESOURCE \
    --project_id="${PROJECT}" \
    --location="${LOCATION}" \
    remote_connection

echo "Deploying the Cloud Function."
gcloud functions deploy sampleCF \
    --project="${PROJECT}" \
    --runtime=python39 \
    --entry-point=remote_vertex_ai \
    --source=call_nlp \
    --trigger-http

echo "Setting the service account."
SERVICE_ACCOUNT=$(bq show --connection --project_id=$PROJECT --location=$LOCATION --format=json remote_connection | jq '.cloudResource.serviceAccountId' -r)
gcloud functions add-iam-policy-binding sampleCF \
    --project="${PROJECT}" \
    --member=serviceAccount:"${SERVICE_ACCOUNT}" \
    --role=roles/cloudfunctions.invoker

echo "Creating the BigQuery UDF."
ENDPOINT=$(gcloud functions describe sampleCF --format="value(httpsTrigger.url)")
NLP_QUERY="""
CREATE OR REPLACE  FUNCTION \`${PROJECT}.${DATASET}.call_nlp\` (x STRING)
RETURNS STRING
REMOTE WITH CONNECTION \`${PROJECT}.${LOCATION}.remote_connection\`
OPTIONS(
  endpoint = '${ENDPOINT}',
  user_defined_context = [(\"mode\",\"call_nlp\")]
);
"""

bq query \
    --location="$LOCATION" \
    --use_legacy_sql=false \
    "${NLP_QUERY}"
