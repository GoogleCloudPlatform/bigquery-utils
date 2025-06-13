# Multimodal UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions) of mutimodal data.
Which deal with [ObjectRef](https://cloud.google.com/bigquery/docs/reference/standard-sql/objectref_functions) of unstructured data in GCS. 
Each UDF within thisdirectory will be automatically synchronized to the `bqutil` project within the
`mm` dataset for reference in queries.

## UDFs

* [exif](#exif)

## Documentation

### [exif(src_obj_ref STRUCT<uri STRING, version STRING, authorizer STRING, details JSON>)](exif.sqlx)
Extract EXIF data as JSON from the ObjectRef of GCS files. Now only support image types.

```sql
SELECT bqutil.mm.exif(
  OBJ.MAKE_REF("gs://<PATH>/<TO>/<YOUR_IMAGE_FILE>", "<BQ_LOCATION>.<YOUR_CONNECTION_ID>")
) AS output_json
```

results:

| output_json |
| ----------- |
| {"ExifOffset": 47, "Make": "MyCamera"} |