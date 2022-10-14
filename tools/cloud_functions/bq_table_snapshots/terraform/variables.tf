
variable "project_id" {
    description = "GCP Project ID containing Cloud Functions and Pub/Sub Topics"
    type        = string
}

variable "storage_project_id" {
    description = "GCP Project ID containing BigQuery tables"
    type        = string
}

variable "region" {
    description = "GCP region in which to deploy cloud function"
    default     = "us-central1"
}

variable "source_dataset_name" {
    description = "Dataset for which snapshots will be created"
    type        = string
}

variable "target_dataset_name" {
    description = "Dataset where the snapshots will be written to"
    type        = string
}

variable "crontab_format" {
    description = "Crontab schedule under which the solution will be executed"
    type        = string
}

variable "seconds_before_expiration" {
    description = "Seconds before the snapshot will expire"
    type        = number
}

variable "tables_to_include_list" {
    type    = string
    default = "[]" 
}

variable "tables_to_exclude_list" {
    type    = string
    default = "[]" 
}