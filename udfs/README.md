# BigQuery UDFs

User-defined functions
([UDFs](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions))
are a feature of SQL supported by BigQuery that enables a user to create a
function using another SQL expression or JavaScript. These functions accept
columns of input and perform actions, returning the result of those actions as a
value.

## Community and Migration Functions

The [community](/udfs/community) folder contains community-contributed functions
that perform some actions in BigQuery. The [migration](/udfs/migration) folder
contains sub-folders such as [teradata](/udfs/migration/teradata),
[redshift](/udfs/migration/redshift), and [oracle](/udfs/migration/oracle) which
contain community-contributed functions that replicate the behavior of
proprietary functions in other data warehouses. These functions can help you
achieve feature parity in a migration from another data warehouse to BigQuery.

## Using the UDFs

All UDFs within this repository are available under the `bqutil` project on
publicly shared datasets. Queries can then reference the shared UDFs via
`bqutil.<dataset>.<function>()`.

![Alt text](/images/public_udf_architecture.png?raw=true "Public UDFs")

## Deploying the UDFs

All UDFs within this repository are maintained in SQLX format. This format is
used to enable testing and deployment of the UDFs with
the [Dataform CLI tool](https://docs.dataform.co/dataform-cli). \
The Dataform CLI is a useful tool for deploying the UDFs because it:

* Enables unit testing the UDFs
* Automatically identifies dependencies between UDFs and then creates them in
  the correct order.
* Easily deploys the UDFs across different environments (dev, test, prod)

The following sections cover a few methods of deploying the UDFs. 

### Deploy with Cloud Build (Recommended)

1. Authenticate using the Cloud SDK and set the BigQuery project in which you'll
   deploy your UDF(s):

   ```bash 
   gcloud init
   ```

1. Enable the Cloud Build API and grant the default Cloud Build service account
   the BigQuery Job User and Data Editor roles
   ```bash
   gcloud services enable cloudbuild.googleapis.com && \
   gcloud projects add-iam-policy-binding \
     $(gcloud config get-value project) \
     --member=serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")"@cloudbuild.gserviceaccount.com" \
     --role=roles/bigquery.user && \
   gcloud projects add-iam-policy-binding \
     $(gcloud config get-value project) \
     --member=serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")"@cloudbuild.gserviceaccount.com" \
     --role=roles/bigquery.dataEditor
   ```
1. Deploy the UDFs by submitting the following:

   ```bash
   # Deploy to US
   gcloud builds submit . --config=deploy.yaml --substitutions _BQ_LOCATION=US
   ```
   > Note: Deploy to a different location by setting `_BQ_LOCATION` to your own
   > desired value.\
   > [Click here](https://cloud.google.com/bigquery/docs/locations#supported_regions)
   > for a list of supported locations.

### Deploy with your own machine

Run the following in your machine's terminal to deploy all UDFs in your own
BigQuery project.

1. Authenticate using the Cloud SDK and set the BigQuery project in which you'll
   deploy your UDF(s):

   ```bash 
   gcloud init
   ```

1. Install the dataform CLI tool:

   ```bash
   npm i -g @dataform/cli
   ```

1. Set env variable BQ_LOCATION to the BigQuery location in which you want to
   deploy the UDFs and then run the `deploy.sh` helper script to deploy the
   UDFs:

   ```bash
   # Deploy to US
   export BQ_LOCATION=US && bash deploy.sh
   ```
   > Note: Deploy to a different location by setting `BQ_LOCATION` to your own
   > desired value.\
   > [Click here](https://cloud.google.com/bigquery/docs/locations#supported_regions)
   > for a list of supported locations.

### Deploy with bq command-line tool or BigQuery Console

If you want to create the UDFs from this repository using the bq command-line
tool, then you must make a few modifications to the SQLX files as shown below:

1. Remove the first line `config { hasOutput: true }` in each SQLX file.
1. Replace any instance of `${self()}` with the fully qualified UDF name.
1. Replace any instance of `${ref(SOME_UDF_NAME)}` with the fully qualified UDF
   name of `SOME_UDF_NAME`.
1. Deploy the UDF using either of the following:
    * bq command-line tool:
      ```bash
      bq query --nouse_legacy_sql < UDF_SQL_FILE_NAME.sqlx`
      ```
    * BigQuery Console: Just paste the SQL UDF body in the console and execute.

### Using JavaScript UDFs

When
creating [JavaScript UDFs](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions#javascript-udf-structure)
in your dataset, you need both to create the UDF and optionally copy the
javascript library to your own Google Storage Bucket.

The base route for all the compiled JS libraries
is `gs://bqutil-lib/bq_js_libs/`.

In the following example, we show how to create in your dataset the Levenshtein
UDF function, that uses the `js-levenshtein-v1.1.6.js` library.

1. Copy the compiled library to your bucket:
   `gsutil cp gs://bqutil-lib/bq_js_libs/js-levenshtein-v1.1.6.js gs://your-bucket`
2. Give permissions to the library. First, if you don't
   have [uniform bucket-level access](https://cloud.google.com/storage/docs/using-uniform-bucket-level-access)
   in your bucket, enable
   it: `gsutil uniformbucketlevelaccess set on gs://your-bucket`. Once done give
   the [Cloud Storage Object Viewer role](https://cloud.google.com/storage/docs/access-control/iam-roles)
   at
   the [bucket](https://cloud.google.com/storage/docs/access-control/using-iam-permissions#bucket-add)
   or [project](https://cloud.google.com/sdk/gcloud/reference/projects/add-iam-policy-binding)
   level to a user or
   group: `gsutil iam ch [user|group]:[user|group]@domain.com:roles/storage.objectViewer gs://your_bucket`
3. Edit the [levenshtein.sql](community/levenshtein.sql) SQL file and replace
   the library path `library="${JS_BUCKET}/js-levenshtein-v1.1.6.js"` with your
   own path `library="gs://your-bucket/js-levenshtein-v1.1.6.js`
4. Create the SQL UDF passing the previously modified SQL file:
   `bq query --project_id YOUR_PROJECT_ID --dataset_id YOUR_DATASET_ID --nouse_legacy_sql < levenshtein.sql`

## Contributing UDFs

If you are interested in contributing UDFs to this repository, please see the
[instructions](/udfs/CONTRIBUTING.md) to get started.
