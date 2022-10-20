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
```
PROJECT=your_project_id
LOCATION=your_bigquery_dataset_location
DATASET=your_bigquery_dataaset_id
```

### Navigate to the remote_udfs directory
Change to the remote_udfs directory after cloning the repo. 
```
cd bigquery-utils/udfs/remote_udfs/nlp
```

### Creating your BigQuery connection 

To create your BigQuery connection, you need to specify the following:
* project_id - the project id that you wish to create the connection in
* location - the location for the external connection in BigQuery

```
bq mk --connection --display_name=\'remote_connection\' --connection_type=CLOUD_RESOURCE \
      --project_id=$PROJECT --location=$LOCATION remote_connection
```

### Deploying your Cloud Function

[More information about deploying your cloud function can be found here.](https://cloud.google.com/functions/docs/deploy)

Snippet provided below for brevity.  
It’s recommended that you keep the default authentication instead of allowing unauthenticated invocation of your Cloud Function or Cloud Run service.  
We use gen1 Cloud Functions here for the simple demo purposes; however, gen2 Cloud Functions are recommended.  

* project - the project the cloud function is deployed to 
* runtime - this was defaulted to python39 but can be changed as required 
```
gcloud functions deploy sampleCF \
--project=$PROJECT --runtime=python39 --entry-point=remote_vertex_ai --source=call_nlp --trigger-http
```

### Granting the service account invoker permissions on the functions

Grant the service account obtained above permissions to invoke the functions.
* project - the project where your external connection was created
* cf_name - the name of your cloud function 
* service_account - the service account associated with the external connection

**_NOTE:_** For the below command you will need to have jq installed. 

You need to grant the connection service account to be able to invoke functions.  
To do this, first obtain the service account.  
Then apply the cloudfunctions.invoker role to the service account for the function. 

```
SERVICE_ACCOUNT=$(bq show --connection --project_id=$PROJECT --location=$LOCATION --format=json remote_connection | jq '.cloudResource.serviceAccountId' -r)
gcloud --project=$PROJECT functions add-iam-policy-binding sampleCF --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/cloudfunctions.invoker
```

### Creating your BigQuery UDF on BigQuery

You can choose to do this many ways.  
* [Creating your BigQuery function](/udfs/remote_udfs/nlp/create_bq_function.sh) 

The input parameters for the helper script above need to be in order:
* project - the project you deployed to 
* dataset - the BigQuery dataset you want to deploy to 
* location - the location where your BigQuery dataset is
* endpoint - the full endpoint of the cloud function invocation

```
ENDPOINT=$(gcloud functions describe sampleCF --format="value(httpsTrigger.url)")

sh create_bq_function.sh $PROJECT $DATASET $LOCATION $PROJECT.$LOCATION.remote_connection $ENDPOINT
```

### Deploying it all together

* [Deploying it all together](/udfs/remote_udfs/nlp/deploy.sh)
This script is a cumulation of the various steps.  
This step is optional, only if you haven't done the steps above.  

```
PROJECT=your_project_id
LOCATION=your_bigquery_dataset_location
DATASET=your_bigquery_dataaset_id

sh deploy.sh $PROJECT $LOCATION $DATASET
```

### Running it on BigQuery
You should now be able to run the remote UDF on BigQuery.

Try it in your BigQuery console. 
```
select `%your_project_id%.%your_dataset_id%.%function_name%`("This is really awesome!");
```

It should return a positive float value (as in greater than 0) as a response.
