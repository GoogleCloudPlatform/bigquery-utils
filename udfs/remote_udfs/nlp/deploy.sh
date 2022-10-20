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
gcloud functions deploy analyze-sentiment \
    --project="${PROJECT}" \
    --region=$LOCATION \
    --runtime=python39 \
    --entry-point=analyze_sentiment \
    --source=call_nlp \
    --trigger-http

echo "Setting the service account."
SERVICE_ACCOUNT=$(bq show --connection --project_id=$PROJECT --location=$LOCATION --format=json remote_connection | jq '.cloudResource.serviceAccountId' -r)
gcloud functions add-iam-policy-binding analyze-sentiment \
    --project="${PROJECT}" \
    --member=serviceAccount:"${SERVICE_ACCOUNT}" \
    --role=roles/cloudfunctions.invoker

echo "Creating the BigQuery UDF."
ENDPOINT=$(gcloud functions describe analyze-sentiment --format="value(httpsTrigger.url)")
REMOTE_UDF_DDL="""
CREATE OR REPLACE  FUNCTION \`${PROJECT}.${DATASET}.analyze_sentiment_plain_text\` (x STRING)
RETURNS STRING
REMOTE WITH CONNECTION \`${PROJECT}.${LOCATION}.remote_connection\`
OPTIONS(
  endpoint = '${ENDPOINT}',
  user_defined_context = [(\"documentType\",\"PLAIN_TEXT\")]
);
"""

bq query \
    --location="$LOCATION" \
    --use_legacy_sql=false \
    "${REMOTE_UDF_DDL}"
