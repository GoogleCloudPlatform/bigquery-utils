""" Contains functions to upload data to Google Cloud Storage and Google
Bigquery. These functions take input specifying where to store the file
and return a message denoting success or error.
"""

from google.cloud import bigquery, storage

def create_bigquery_table(project_id, dataset_id, table_id):
    """ Creates a table in Google BigQuery

    Args:
        project_id: Google Cloud Project ID for destination.
        dataset_id: Name of destination dataset.
        table_id: Name of destination table.

    Returns:
        True on success, False otherwise
    """

    client = bigquery.Client(project=project_id)

    schema = [
        bigquery.SchemaField("Query", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("URL", "STRING", mode="REQUIRED"),
    ]

    table_id = "{}.{}.{}".format(project_id, dataset_id, table_id)
    table = bigquery.Table(table_id, schema=schema)
    try:
        table = client.create_table(table)
    except Exception as e:
        return False
    return True

def insert_rows(project_id, dataset_id, table_id, data):
    """ Inserts rows into BigQuery table

    Args:
        project_id: Google Cloud Project ID for destination.
        dataset_id: Name of destination dataset.
        table_id: Name of destination table.
        data: Rows to be inserted

    Returns:
        List of errors during upload, empty list if successful
    """

    client = bigquery.Client(project=project_id)
    table_id = "{}.{}.{}".format(project_id, dataset_id, table_id)

    table = client.get_table(table_id)
    errors = client.insert_rows(table, data)
    return errors

def load_bigquery_table(project_id, dataset_id, table_id, filename):
    """ Uploads a file to Google BigQuery.

    Args:
        project_id: Google Cloud Project ID for destination.
        dataset_id: Name of destination dataset.
        table_id: Name of destination table.
        filename: Name of CSV file to be uploaded.

    Returns:
        A log message depending on success or failure.
    """

    client = bigquery.Client(project=project_id)

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    job_config.schema = [
        bigquery.SchemaField("query", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
    ]

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    try:
        job.result()
    except Exception as e:
        return False, job.errors
    return True, "Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id)

def upload_gcs_file(project_id, bucket_id, destination_blob_name, filename):
    """ Uploads a file to Google Cloud Storage.

    Args:
        project_id: Google Cloud Project ID for destination.
        bucket_id: Name of destination bucket.
        destination_blob_name: Name of destination file.
        filename: Name of file to be uploaded.

    Returns:
        A log message depending on success or failure.
    """

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_id)
    blob = bucket.blob(destination_blob_name)

    try:
        blob.upload_from_filename(filename)
    except Exception as e:
        return False, e

    return True, "File {} uploaded to {}.".format(filename, destination_blob_name)
