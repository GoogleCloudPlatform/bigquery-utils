1. Setup deployment configuration

    ``` bash
    export PROJECT_ID=""  # Project ID where resources are created
    export DATASET_ID=""
    export REGION="us-central1"  # Region for Artifact Registry, Cloud Run and Cloud Scheduler
    export REPOSITORY="bigquery-antipattern-recognition"  # Artifact Registry repository name

    export CONTAINER_IMAGE="$REGION-docker.pkg.dev/$PROJECT_ID/$REPOSITORY/recognizer:0.1.1-SNAPSHOT"

    export CLOUD_RUN_JOB_NAME="bigquery-antipattern-recognition"  # Name for the Cloud Run job
    export CLOUD_RUN_JOB_SA=""  # Service account associated to the Cloud Run job
    export OUTPUT_TABLE=""  # Ex: "project.dataset.table" BigQuery output table for the Anti Pattern Detector
    ```
2. Create BQ Antipattern output table
    ``` bash
    bq mk \
    -t \
    $DATASET.antipattern_output_table \
    antipattern_output_schema.json
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

   This example configuration reads all queries for the previous day from `INFORMATION_SCHEMA`, runs antipattern detection and writes the result to the configured `OUTPUT_TABLE`.

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
    gcloud workflows deploy hashAntiPattern --source=mhash_anti_pattern_workflow.yaml \
    --service-account=$CLOUD_RUN_JOB_SA
    
    ```