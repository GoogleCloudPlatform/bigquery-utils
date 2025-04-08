# BigQuery UDFs

User-defined functions
([UDFs](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions))
are a feature of SQL supported by BigQuery that enables a user to create a
function using another SQL expression or JavaScript. These functions accept
columns of input and perform actions, returning the result of those actions as a
value.

> [!CAUTION]
> Do not directly reference any `bqutil` UDFs in your production environment.
> You should instead [deploy these UDFs](#deploying-the-udfs) into your 
> production environment. 
> 
> UDFs in this repo are hosted in `bqutil` datasets to help you easily test and
> demo their functionality. Any updates to UDFs in this repo may result in 
> breaking changes since only the latest version of each UDF is deployed to the
> `bqutil` datasets. 

## Repo Folder to BigQuery Dataset Mappings

> [!IMPORTANT]
> The UDF datasets listed below reside in US multi-region, but are also available in all other supported BigQuery locations as described in the [Using the UDFs](#using-the-udfs) section.

| Repo Folder                                         | BigQuery UDF Dataset                                                                                       | Description                                                                                                                                                                                                                                                                                                                            |
|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`community/`](/udfs/community)                     | [`bqutil.fn`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2sfn)                     | Contains an assortment of community-contributed functions.                                                                                                                                                                                                                                                                             |
| [`datasketches/`](/udfs/datasketches/)              | [`bqutil.datasketches`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2sdatasketches) | Contains an open source, high-performance library of stochastic streaming algorithms commonly called "sketches". The source for these UDFs are maintained in the apache/datasketches-bigquery repo, but are available in the bqutil.datasketches US multi-region dataset and all other regions as described in Using the UDFs section. |
| [`migration/oracle/`](/udfs/migration/oracle)       | [`bqutil.or`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2sor)                     | Contains community-contributed functions that replicate the behavior of Oracle functions.                                                                                                                                                                                                                                              |
| [`migration/redshift/`](/udfs/migration/redshift)   | [`bqutil.rs`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2srs)                     | Contains community-contributed functions that replicate the behavior of Redshift functions.                                                                                                                                                                                                                                            |
| [`migration/snowflake/`](/udfs/migration/snowflake) | [`bqutil.sf`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2ssf)                     | Contains community-contributed functions that replicate the behavior of Snowflake functions.                                                                                                                                                                                                                                           |
| [`migration/sqlserver/`](/udfs/migration/sqlserver) | [`bqutil.ss`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2sss)                     | Contains community-contributed functions that replicate the behavior of SQL Server functions.                                                                                                                                                                                                                                          |
| [`migration/teradata/`](/udfs/migration/teradata/)  | [`bqutil.td`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2std)                     | Contains community-contributed functions that replicate the behavior of Teradata functions.                                                                                                                                                                                                                                            |
| [`migration/vertica/`](/udfs/migration/vertica)     | [`bqutil.ve`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbqutil!2sve)                     | Contains community-contributed functions that replicate the behavior of Vertica functions.                                                                                                                                                                                                                                             |

## Using the UDFs

All UDFs within this repository are available under the `bqutil` project on
publicly shared datasets. Queries can then reference the shared UDFs in the US multi-region via
`bqutil.<dataset>.<function>()`.

UDFs within this repository are also deployed publicly into every other region that [BigQuery supports](https://cloud.google.com/bigquery/docs/locations). 
In order to use a UDF in your desired location outside of the US multi-region, you can reference it via a dataset with a regional suffix:

`bqutil.<dataset>_<region>.<function>()`

For example, the Teradata `nullifzero` can be referenced in various locations:

```sql
bqutil.td_eu.nullifzero()            -- eu multi-region

bqutil.td_europe_west1.nullifzero()  -- europe-west1 region

bqutil.td_asia_south1.nullifzero()   -- asia-south1 region
```

> [!NOTE]  
> Region suffixes added to dataset names replace `-` with `_` in order to comply with BigQuery dataset naming rules.

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

### Deploy with BigQuery SQL (Fastest)

<details><summary><b>&#128466; Click to expand step-by-step instructions</b></summary>

#### Deploy all the `bqutil.fn` UDFs into your own project:

Run the following `gcloud` command to copy the JavaScript files hosted in the 
`bqutil` project's Cloud Storage bucket to your own bucket:

```bash
# For US multi-region us the following command
gcloud storage cp gs://bqutil-lib/bq_js_libs/* gs://YOUR_BUCKET/
# For other regions, modify the command to use the appropriate bucket.
# Examples shown below:
#
# gcloud storage cp gs://bqutil-lib-eu/* gs://YOUR_BUCKET/
# gcloud storage cp gs://bqutil-lib-asia-east2/* gs://YOUR_BUCKET/
```

Run the following SQL script in your BigQuery console to copy all `bqutil.fn` UDFs into
your own project:

```sql
-- SET YOUR DESIRED BQ REGION BELOW
SET @@location="us-east4";
-- SET YOUR CLOUD STORAGE BUCKET BELOW
DECLARE YOUR_JS_BUCKET STRING DEFAULT("gs://YOUR_BUCKET");
/**********************************
 * DO NOT EDIT SQL BELOW THIS LINE
 **********************************/
DECLARE YOUR_PROJECT_ID STRING DEFAULT("`"||@@project_id||"`");
DECLARE YOUR_REGION STRING DEFAULT(LOWER(@@location));
DECLARE region_suffix STRING DEFAULT(
  IF(YOUR_REGION="us", "", "_" || REPLACE(YOUR_REGION, "-", "_"))
);
-- Get regional UDFs
DECLARE fn_udf_ddls ARRAY<STRING>;
EXECUTE IMMEDIATE
   FORMAT("""
  SELECT ARRAY_AGG(ddl ORDER BY created) AS fn_udf_ddls
  FROM bqutil.fn%s.INFORMATION_SCHEMA.ROUTINES
  """,
          region_suffix
      )
   INTO fn_udf_ddls;
-- Creates the fn dataset within your project
EXECUTE IMMEDIATE "CREATE SCHEMA IF NOT EXISTS " || YOUR_PROJECT_ID || ".fn" || region_suffix;
-- Creates all cw_* UDFs within your new fn dataset
FOR fn_udf_ddl IN (SELECT * FROM UNNEST(fn_udf_ddls) ddl)
DO EXECUTE IMMEDIATE 
  REPLACE(
    REPLACE(
      REPLACE(
        fn_udf_ddl.ddl,
        "gs://bqutil-lib"|| IF(YOUR_REGION <> "us", "-" || @@location, "/bq_js_libs"), YOUR_JS_BUCKET),
      "FUNCTION bqutil.", "FUNCTION " || YOUR_PROJECT_ID || "."),
    "CREATE ", "CREATE OR REPLACE ");
END FOR;
```

#### Deploy all the `bqutil.fn.cw_` prefix UDFs into your own project: 
```sql
-- SET YOUR DESIRED BQ REGION BELOW
SET @@location="us-east4";
/**********************************
 * DO NOT EDIT SQL BELOW THIS LINE
 **********************************/
DECLARE YOUR_PROJECT_ID STRING DEFAULT("`"||@@project_id||"`");
DECLARE YOUR_REGION STRING DEFAULT(LOWER(@@location));
DECLARE region_suffix STRING DEFAULT(
  IF(YOUR_REGION="us", "", "_" || REPLACE(YOUR_REGION, "-", "_"))
);
-- Get regional UDFs
DECLARE cw_udf_ddls ARRAY<STRING>;
EXECUTE IMMEDIATE
   FORMAT("""
  SELECT ARRAY_AGG(ddl ORDER BY created) AS cw_udf_ddls
  FROM bqutil.fn%s.INFORMATION_SCHEMA.ROUTINES
  WHERE specific_name LIKE "cw_%%"
  """,
          region_suffix
      )
   INTO cw_udf_ddls;
-- Creates the fn dataset within your project
EXECUTE IMMEDIATE "CREATE SCHEMA IF NOT EXISTS " || YOUR_PROJECT_ID || ".fn" || region_suffix;
-- Creates all cw_* UDFs within your new fn dataset
FOR cw_udf_ddl IN (SELECT * FROM UNNEST(cw_udf_ddls) ddl)
DO EXECUTE IMMEDIATE 
  REPLACE(
    REPLACE(
      cw_udf_ddl.ddl,
      "FUNCTION bqutil.", "FUNCTION " || YOUR_PROJECT_ID || "."),
    "CREATE ", "CREATE OR REPLACE ");
END FOR;
```

</details>

### Deploy with Cloud Build

<details><summary><b>&#128466; Click to expand step-by-step instructions</b></summary>

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
   gcloud builds submit . --config=deploy.yaml --substitutions _PROJECT_ID=YOUR_PROJECT_ID,_BQ_LOCATION=US
   ```

   > IMPORTANT:
   > Deploy to a different location by setting `_BQ_LOCATION` to your own
   > desired value.\
   > [Click here](https://cloud.google.com/bigquery/docs/locations#supported_regions)
   > for a list of supported locations.

</details>

### Deploy with your own machine

<details><summary><b>&#128466; Click to expand step-by-step instructions</b></summary>

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

   > IMPORTANT:
   > Deploy to a different location by setting `BQ_LOCATION` to your own
   > desired value.\
   > [Click here](https://cloud.google.com/bigquery/docs/locations#supported_regions)
   > for a list of supported locations.

</details>

## Contributing UDFs

![Alt text](/images/public_udf_architecture.png?raw=true "Public UDFs")

If you are interested in contributing UDFs to this repository, please see the
[instructions](/udfs/CONTRIBUTING.md) to get started.
