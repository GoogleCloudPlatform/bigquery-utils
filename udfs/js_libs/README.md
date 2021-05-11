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
    library=["${JS_BUCKET}/LIBRARY-NAME-v1.2.3.min.js"] 
  )
  AS 
  """
  return LIBRARY_NAME.func_from_lib(a, b);
  """;
  ```

**Note: When your UDF makes a call to the javascript library,
 make sure to convert any dashes '-' to underscores '_' in the javascript library name.** (e.g. LIBRARY-NAME in example
 above is invoked as LIBRARY_NAME in the UDF body.)

## Testing js_libs locally

If you'd like to add (or update the version of) a javascript library, you can build js_libs via cloudbuild and upload them to your own GCS bucket.
After updating [js_libs.yaml](js_libs.yaml), execute the cloudbuild_js_libs build, substituting a GCS bucket within your test project accordingly:
```bash
gcloud builds submit . --config=cloudbuild_js_libs.yaml --substitutions=_JS_BUCKET="gs://YOUR_GCS_BUCKET[/optional_sub_path]
```
After the libraries have been built, you'll need to provide the `_JS_BUCKET` environment variable when running your local UDF tests.
For example:
```bash
_JS_BUCKET="gs://YOUR_GCS_BUCKET[/optional_sub_path]" bash tests/run.sh -k kruskal_wallis
```
