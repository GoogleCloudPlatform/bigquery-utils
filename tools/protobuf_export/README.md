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
  <namespace>.toProto(input STRUCT<word STRING,
    wordCount BIGNUMERIC>,
    protoMessage STRING)
  RETURNS BYTES
  LANGUAGE js OPTIONS ( library=["gs://{YOUR_GCS_BUCKET}/pbwrapper.js"] ) AS r"""
let message = pbwrapper.setup(protoMessage)
return pbwrapper.parse(message, input)
 """;
 ```
 7. Use the created function like so:
```
SELECT
  <namespace>.toProto(STRUCT(word,
      CAST(word_count AS BIGNUMERIC)),
    "<proto package name>.<proto message name>") AS protoResult
FROM
  `bigquery-public-data.samples.shakespeare`
```

## Caveats
1. While the same pbwrapper.js can be used for all .proto files under protos folder, you will still need to create one such function per proto message. That is due to the fact that BigQuery structs are fully typed.
