# backfill audit logs
This tool will take the results from a `gcloud logging read`  and create a JSON file that is loadable using `bq load`

# ToDo
Add functionality to allow you to pass in an existing logs router as an argument so you don't have to manually create the `gcloud logging read` command.
