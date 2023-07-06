# BigQuery Protobuf Export

## Introduction
When querying BigQuery, it is sometimes required to export data as protobuf bytes. 
This can be useful for coding purposes, where we may want an elaborate data
type to be exported.

## Procedure
1. Clone this reposiory.
2. Add your proto files under `protos` folder.
3. Run `npm install`.
4. Run `npx webpack --config webpack.config.js --stats-error-details`.
5. Copy the file from dist folder (`dist/pbwrapper.js`) into GCS.
6. Use the created script in a BigQuery User Defined Function like so:

```
CREATE FUNCTION
  <dataset-id>.toMyProtoMessage(input STRUCT<word STRING,
    wordCount BIGNUMERIC>)
  RETURNS BYTES
  LANGUAGE js OPTIONS ( library=["gs://{YOUR_GCS_BUCKET}/pbwrapper.js"] ) AS r"""
let message = pbwrapper.setup("<proto package name>.<proto message name>")
return pbwrapper.parse(message, input)
 """;
 ```
 7. Use the created function like so:
```
SELECT
  <dataset-id>.toMyProtoMessage(STRUCT(word,
      CAST(word_count AS BIGNUMERIC))) AS protoResult
FROM
  `bigquery-public-data.samples.shakespeare`
LIMIT
  100;
```

## Required permissions
The following permissions are required:

1. bigquery.routines.create - Required to create a user defined function. Required one time for running this procedure.
2. storage.objects.create - Required to upload pbwrapper.js to GCS. Required one time for running this procedure.
3. bigquery.tables.export - Required to export data from BigQuery. Required for the user running the query.
4. storage.objects.get - Required to read pbwrapper.js from GCS. Required for the user running the query.


## Caveats
1. While the same pbwrapper.js can be used for all .proto files under protos folder, you will still need to create one such function per proto message. That is due to the fact that BigQuery structs are fully typed.
