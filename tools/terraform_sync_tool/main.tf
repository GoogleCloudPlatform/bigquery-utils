provider "google" {
  project = var.project_id
  region  = "us-central1"
  zone    = "us-central1-c"
}

resource "google_bigquery_dataset" "example_dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "test"
  description                 = "This is a description"
  location                    = "US"
  default_table_expiration_ms = 3600000
}

resource "google_bigquery_dataset_iam_binding" "reader" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  role       = "roles/bigquery.dataViewer"

  members = [
    "user:candicehou@google.com",
  ]
}

resource "google_bigquery_table" "foo" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  table_id   = "foo"
  schema = file("bq_schema.json")
}