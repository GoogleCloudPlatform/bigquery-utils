# Calling Google's Natural Language API Sentiment Analysis Example 

Google's Natural Language API Sentiment Analysis inspects the given text and identifies the prevailing emotional opinion within the text, especially to determine a writer's attitude as positive, negative, or neutral.  

To run this example, you will need the following APIs enabled:
* [BigQuery API](https://cloud.google.com/bigquery/docs)
* [Cloud Functions API](https://cloud.google.com/functions/docs)
* [BigQuery Connections API](https://cloud.google.com/bigquery/docs/working-with-connections#enable_the_connection_service)
* [Natural Language API](https://cloud.google.com/natural-language/docs/setup#api)

## Running the example

### Setting the environment variables 

Replace the various environment variables below with your desired values.  

**_NOTE:_** Your CLOUD_FUNCTION_REGION must match or be within the multi-region of BigQuery LOCATION. 

```
PROJECT=your_project_id
LOCATION=your_bigquery_dataset_location
DATASET=your_bigquery_dataaset_id
CLOUD_FUNCTION_REGION=your_cloud_function_region
```

### Navigate to the remote_udfs directory
Change to the remote_udfs directory after cloning the repo. 
```
cd bigquery-utils/udfs/remote_udfs/examples/nlp
```

### Create your BigQuery dataset
Run the following command to create your BigQuery dataset:
```
bq --location="${LOCATION}" mk -d --project_id="${PROJECT}" "${DATASET}"
```

### Creating your BigQuery connection 

Run the following command to create your BigQuery external connection:

```
bq mk --connection \
    --display_name=\'remote_connection\' \
    --connection_type=CLOUD_RESOURCE \
    --project_id="${PROJECT}" \
    --location="${LOCATION}" \
    remote_connection
```

### Deploying your Cloud Function

[More information about deploying your cloud function can be found here.](https://cloud.google.com/functions/docs/deploy)

Snippet provided below for brevity.  
It’s recommended that you keep the default authentication instead of allowing unauthenticated invocation of your Cloud Function or Cloud Run service.  
We use gen1 Cloud Functions here for the simple demo purposes; however, gen2 Cloud Functions are recommended. 
Run the following command to deploy your Cloud Function:
```
gcloud functions deploy analyze-sentiment \
    --project="${PROJECT}" \
    --runtime=python39 \
    --entry-point=analyze_sentiment \
    --region="${CLOUD_FUNCTION_REGION}" \
    --source=call_nlp \
    --trigger-http
```

### Granting the service account invoker permissions on the functions


**_NOTE:_** For the below commands you will need to have jq installed.  

Run the following commands to grant the service account obtained above permissions to invoke your Cloud Function:

```
SERVICE_ACCOUNT=$(bq show --connection --project_id="${PROJECT}" --location="${LOCATION}" --format=json remote_connection | jq '.cloudResource.serviceAccountId' -r)
gcloud functions add-iam-policy-binding analyze-sentiment \
    --project="${PROJECT}" \
    --member=serviceAccount:"${SERVICE_ACCOUNT}" \
    --role=roles/cloudfunctions.invoker
```

### Creating your BigQuery UDF

You are able to provide different context to the Cloud Function as key value pairs. This example uses context to specify the language of text being passed to the NLP API. 
[You can read more about it here.](https://cloud.google.com/bigquery/docs/reference/standard-sql/remote-functions#providing_user_defined_context)

Run the following DDL statements to create two remote UDFs in BigQuery, one which analyzes sentiment in English and one in Spanish.

```
ENDPOINT=$(gcloud functions describe analyze-sentiment --format="value(httpsTrigger.url)")

ANALYZE_en_UDF_DDL="""
CREATE OR REPLACE  FUNCTION \`${PROJECT}.${DATASET}.analyze_sentiment_en\` (x STRING)
RETURNS STRING
REMOTE WITH CONNECTION \`${PROJECT}.${LOCATION}.remote_connection\`
OPTIONS(
  endpoint = '${ENDPOINT}',
  user_defined_context = [(\"language\",\"en\")]
);"""

ANALYZE_es_UDF_DDL="""
CREATE OR REPLACE  FUNCTION \`${PROJECT}.${DATASET}.analyze_sentiment_es\` (x STRING)
RETURNS STRING
REMOTE WITH CONNECTION \`${PROJECT}.${LOCATION}.remote_connection\`
OPTIONS(
  endpoint = '${ENDPOINT}',
  user_defined_context = [(\"language\",\"es\")]
);
"""

bq query \
    --location="${LOCATION}" \
    --use_legacy_sql=false \
    "${ANALYZE_en_UDF_DDL}"
    
bq query \
    --location="${LOCATION}" \
    --use_legacy_sql=false \
    "${ANALYZE_es_UDF_DDL}"    
```

### Putting it all together

The [deploy.sh](/udfs/remote_udfs/examples/nlp/deploy.sh) script combines all the previous setup steps into one simple script which you can execute via the following command:  

**_NOTE:_** Your CLOUD_FUNCTION_REGION must match or be within the multi-region of BigQuery LOCATION. 

```
sh deploy.sh "${PROJECT}" "${LOCATION}" "${DATASET}" "${CLOUD_FUNCTION_REGION}"
```

### Running it on BigQuery
You should now be able to run the remote UDF on BigQuery.

**_NOTE:_** If you get a permissions denied error, wait approximately 60 seconds, permissions may take up to 60 seconds to propagate. 

Try it in your BigQuery console. 
```
SELECT
    `your_project_id.your_dataset_id.analyze_sentiment_en`("This is really awesome!") AS english_positive_sentiment,
    `your_project_id.your_dataset_id.analyze_sentiment_es`("esto es realmente impresionante") AS spanish_positive_sentiment,
    `your_project_id.your_dataset_id.analyze_sentiment_en`("This is really bad!") AS english_negative_sentiment,
    `your_project_id.your_dataset_id.analyze_sentiment_es`("esto es realmente malo") AS spanish_negative_sentiment
```
