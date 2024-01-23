1. Setup deployment configuration

    ``` bash
    gcloud services enable run.googleapis.com
    gcloud services enable workflows.googleapis.com

    export PROJECT_ID=""  # Project ID where resources are created
    export DATASET_ID="optimization_workshop"
    export REGION="us-central1"  # Region for Artifact Registry, Cloud Run and Cloud Scheduler
    export REPOSITORY="bigquery-antipattern-recognition"  # Artifact Registry repository name

    export CONTAINER_IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/recognizer:0.1.1-SNAPSHOT"

    export CLOUD_RUN_JOB_NAME="bigquery-antipattern-recognition"  # Name for the Cloud Run job
    export CLOUD_RUN_JOB_SA=""  # Service account associated to the Cloud Run job
    export INPUT_TABLE="$PROJECT_ID.$DATASET_ID.hash_raw"
    export OUTPUT_TABLE="$PROJECT_ID.$DATASET_ID.antipattern_output_table"  # Ex: "project.dataset.table" BigQuery output table for the Anti Pattern Detector
    ```
2. Create BQ dataset + Antipattern output table
    ``` bash
    bq --location=US mk \
    -d \
    $DATASET_ID

    bq mk \
    -t \
    $OUTPUT_TABLE \
    ./antipattern_output_schema.json
    ```

2. Create an Artifact Registry Repository, if necessary

    ``` bash
    gcloud artifacts repositories create $REPOSITORY \
        --repository-format=docker \
        --location=$REGION \
        --project=$PROJECT_ID
    ```

3. Build and push the container image to Artifact Registry

    ``` bash
    gcloud auth configure-docker $REGION-docker.pkg.dev

    git clone https://github.com/GoogleCloudPlatform/bigquery-antipattern-recognition.git
    cd bigquery-antipattern-recognition

    mvn clean package jib:build \
        -DskipTests \
        -Djib.to.image=$CONTAINER_IMAGE
    ```

4. Create the Cloud Run Job

    ``` bash
    gcloud run jobs create $CLOUD_RUN_JOB_NAME \
        --image=$CONTAINER_IMAGE \
        --max-retries=3 \
        --task-timeout=15m \
        --args="--input_bq_table" --args="$INPUT_TABLE" \
        --args="--output_table" --args="$OUTPUT_TABLE" \
        --service-account=$CLOUD_RUN_JOB_SA \
        --region=$REGION \
        --project=$PROJECT_ID
    ```

5. Deploy workflow

    ``` bash
    gcloud workflows deploy hashAntiPattern --source=hash_antipattern_workflow.yaml \
    --service-account=$CLOUD_RUN_JOB_SA

    ```

6. Run workflow

    ``` bash
    gcloud workflows run hashAntiPattern \
    --data='{"project":"afleisc-bng-dev"}'
    ```