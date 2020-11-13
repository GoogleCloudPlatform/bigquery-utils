# Adding a Javascript Library for use in a BigQuery UDF

In order to host an npm package in the bigquery-utils GCS bucket, add the npm package name and version
to the [js_libs.yaml](js_libs.yaml) file. Make sure the npm package name is exactly as it's listed
in the [npm registry](https://www.npmjs.com/).
 
You can then import the npm package into your BigQuery UDF using the following syntax:
```
CREATE FUNCTION myFunc(a FLOAT64, b STRING)
  RETURNS STRING
  LANGUAGE js
  OPTIONS ( 
    library=["gs://bqutil-lib/bq_js_libs/LIBRARY_NAME-v1.2.3.min.js"] 
  )
  AS 
  """
  return LIBRARY_NAME.func_from_lib(a, b);
  """;
  ```

