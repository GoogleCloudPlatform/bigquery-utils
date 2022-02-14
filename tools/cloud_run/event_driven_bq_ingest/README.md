# Cloud Run Event Driven Pipeline Tutorial

This sample shows how to create an event driven pipeline on Cloud Run via Pub/Sub

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.png)](https://ssh.cloud.google.com/cloudshell/open?cloudshell_git_repo=https://github.com/yrvine-g/event-driven-pipeline&cloudshell_tutorial=pipeline/pubsub/README.md)



## Dependencies

* **Spring Boot**: Web server framework.
* **Jib**: Container build tool.

## Setup
Create  GCS bucket and a folder path with the following format:
gs://bucket/project/dataset/table_name/*.avro

Enable following APIs: 
* container registry 
* cloud run handler

```sh
gcloud config set project MY_PROJECT
```

Configure environment variables:

```sh
export MY_RUN_SERVICE=run-service
export MY_RUN_CONTAINER=run-container
export PROJECT=$(gcloud config get-value project)
export MY_GCS_BUCKET="$(gcloud config get-value project)-gcs-bucket"
export REGION=us-central1
export SERVICE_ACCOUNT=cloud-run-pubsub-invoker
```

## Quickstart

Use the [Jib Maven Plugin](https://github.com/GoogleContainerTools/jib/tree/master/jib-maven-plugin) to build and push container image:

Switch to directory with .pom

```sh
mvn compile jib:build -Dimage=gcr.io/$PROJECT/$MY_RUN_CONTAINER
```

Deploy Cloud Run service:
```sh
gcloud config set run/region $REGION
gcloud run deploy $MY_RUN_SERVICE \
--image gcr.io/$PROJECT/$MY_RUN_CONTAINER \
--no-allow-unauthenticated
```


Create PubSub Topic and GCS notification
```sh
gcloud pubsub topics create pipelineNotification

gsutil notification create -f json -t pipelineNotification -e OBJECT_FINALIZE gs://"$MY_GCS_BUCKET"
```


Create Service Account for Cloud Run and Pub/Sub permissions
```sh
gcloud iam service-accounts create $SERVICE_ACCOUNT \
--display-name "Cloud Run Pub/Sub Invoker"
gcloud run services add-iam-policy-binding $MY_RUN_SERVICE  \ 
--member=serviceAccount:$SERVICE_ACCOUNT@$PROJECT.iam.gserviceaccount.com \
--role=roles/run.invoker
```

Create Pub/Sub Subscription
```sh
export RUN_SERVICE_URL=$(gcloud run services describe $MY_RUN_SERVICE --format='value(status.url)')
gcloud pubsub subscriptions create pipelineTrigger --topic pipelineNotification \  
 --push-endpoint=$RUN_SERVICE_URL \
 --push-auth-service-account=$SERVICE_ACCOUNT@$PROJECT.iam.gserviceaccount.com \
 --ack-deadline=600
```


Create log sink
```shell
export PROJECT_NO=$(gcloud projects list --filter="$PROJECT" --format="value(PROJECT_NUMBER)")
gcloud logging sinks create bq-job-completed \
pubsub.googleapis.com/projects/$PROJECT/topics/pipelineNotification \
 --log-filter='resource.type="bigquery_project" 
severity=INFO 
protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE" 
protoPayload.authenticationInfo.principalEmail="'$PROJECT_NO'-compute@developer.gserviceaccount.com"'
export LOG_SERVICE_ACCOUNT=$(gcloud logging sinks describe bq-job-completed --format='value(writerIdentity)')
gcloud pubsub topics add-iam-policy-binding pipelineNotification \
     --member=serviceAccount:LOG_SERVICE_ACCOUNT --role=roles/pubsub.publisher 
```
