# Get Started with Protobuf Export!

## Let's get started!

This guide will show you how to export protobuf columns.

**Time to complete**: About 10-15 minutes

Click the **Start** button to move to the next step.

## Run the following to create pbwrapper.js.

Now that you've already downloaded the example to Cloud Shell and you're in the
example directory, you're ready to run the commands to get
this example up and running. Follow these steps to run this example:

### 1. Navigate to protobuf_export folder
```bash
cd tools/protobuf_export
```

### 2. Install the JavaScript dependencies in the Cloud Shell terminal:

```bash
npm install
```

### 3. Generate the pbwrapper.js file by running the following:

```bash
npx webpack --config webpack.config.js --stats-error-details
```

### 4. Upload pbwrapper.js to GCS:
```bash
gcloud storage cp dist/pbwrapper.js gs://DESTINATION_BUCKET_NAME/
```

Congratulations, you successfully uploaded pbwrapper.js to GCS!

To use pbwrapper.js, click **Next**.

## Use pbwrapper.js to export protobuf columns

### 1. Create a user-defined function that uses pbwrapper.js:

```sql
bq query --use_legacy_sql=false \
'CREATE FUNCTION 
  mynamespace.toProto(input STRUCT<dummyField STRING>, 
    protoMessage STRING) 
  RETURNS BYTES 
  LANGUAGE js OPTIONS ( library=["gs://{DESTINATION_BUCKET_NAME}/pbwrapper.js"] ) AS r""" 
let message = pbwrapper.setup(protoMessage); 
return pbwrapper.parse(message, input) 
 """;' 
```

### 2. Use your newly created user-defined function to get protobuf columns

```sql
bq query --use_legacy_sql=false \
'SELECT 
  mynamespace.toProto(STRUCT(word), 
    "dummypackage.DummyMessage") 
FROM 
  bigquery-public-data.samples.shakespeare' 
```

## Congratulations 🎉

You have successfully queried a protobuf column from BigQuery!
