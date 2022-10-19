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
CONNECTION_NAME=%Functional BigQuery Connection name% 
DISPLAY_NAME=%Friendly or Display BigQuery Connection name%
PROJECT=%Your project name% 
LOCATION=%Your BigQuery dataset location% 
CF_NAME=%Your Cloud Function name%
ENTRY_POINT=%Your Cloud Function Entry Point%
DATASET=%Your BigQuery Dataset to deploy to%
```

### Navigate to the remote_udfs directory
Change to the remote_udfs directory after cloning the repo. 
```
cd bigquery-utils/udfs/remote_udfs/ 
```

### Creating your BigQuery connection 

To create your BigQuery connection, you need to specify the following:
* project_id - the project id that you wish to create the connection in
* location - the location for the external connection in BigQuery
* connection_name - the name of the connection 
* display_name - (optional) the name (friendly name) you want to show 

```
bq mk --connection --display_name=\'$DISPLAY_NAME\' --connection_type=CLOUD_RESOURCE \
      --project_id=$PROJECT --location=$LOCATION $CONNECTION_NAME
```

### Getting the service account associated with the connection

You need to grant the connection service account to be able to invoke functions.  
To do this, first obtain the service account. 
* location - the location for the external connection in BigQuery
* project - the project where your external connection was created
* connection_name - the name (or friendly/display name) of the external connection

```
bq show --connection --project_id=$PROJECT --location=$LOCATION $CONNECTION_NAME
```

Within properties you'll find *serviceAccountId* which will have the service ID you'll need in a subsequent step.

### Deploying your Cloud Function

[More information about deploying your cloud function can be found here.](https://cloud.google.com/functions/docs/deploy)

Snippet provided below for brevity.  
It’s recommended that you keep the default authentication instead of allowing unauthenticated invocation of your Cloud Function or Cloud Run service.

* cf_name - the name of the cloud function
* project - the project the cloud function is deployed to 
* runtime - this was defaulted to python39 but can be changed as required 
```
gcloud functions deploy $CF_NAME \
--project=$PROJECT --runtime=python39 --entry-point=$ENTRY_POINT --source=call_nlp --trigger-http
```

### Granting the service account invoker permissions on the functions

Grant the service account obtained above permissions to invoke the functions.
* project - the project where your external connection was created
* cf_name - the name of your cloud function 
* service_account - the service account obtained above
```
gcloud --project=$PROJECT functions add-iam-policy-binding $CF_NAME --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/cloudfunctions.invoker
```

### Obtaining the full Cloud Function endpoint

[More information about the gcloud functions describe can be found here.](https://cloud.google.com/sdk/gcloud/reference/functions/describe)
* cf_name - the name of the cloud function
```
gcloud functions describe $CF_NAME
```

You will need the full URL under the httpsTrigger section.
It should look something like this:
```
https://<location>-<project-id>.cloudfunctions.net/<cf-name>
```

### Creating your BigQuery UDF on BigQuery

You can choose to do this many ways.  
* [creating your bq function](/remote_udfs/nlp/create_bq_function.sh) 

The input parameters for the helper script above need to be in order:
* project - the project you deployed to 
* dataset - the BigQuery dataset you want to deploy to 
* location - the location where your BigQuery dataset is 
* connection_name - the name of the connection 
* endpoint - the full endpoint you obtained above

```
sh create_bq_function.sh $PROJECT $DATASET $LOCATION $CONNECTION_NAME %endpoint%
```

### Running it on BigQuery
You should now be able to run the remote UDF on BigQuery.

Try it in your BigQuery console. 
```
select function_name("This is really awesome!");
```

It should return a positive float value (as in greater than 0) as a response.

