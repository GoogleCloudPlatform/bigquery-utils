PROJECT=$1
DATASET=$2
LOCATION=$3
CONNECTION_NAME=$4
ENDPOINT=$5

NLP_QUERY=" CREATE OR REPLACE  FUNCTION \`$PROJECT.$DATASET.call_nlp\` (x STRING) RETURNS STRING REMOTE WITH CONNECTION \`$CONNECTION_NAME\` OPTIONS (endpoint = '$ENDPOINT', user_defined_context = [(\"mode\",\"call_nlp\")])"

# Requires the bq CLI installed.

echo $NLP_QUERY
bq --location=$LOCATION query \
--use_legacy_sql=false \
$NLP_QUERY