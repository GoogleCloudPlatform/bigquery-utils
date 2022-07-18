variable "location" {
  description = "The regional location for the dataset only US and EU are allowed in module"
  type        = string
  default     = "US"
}

variable "deletion_protection" {
  description = "Whether or not to allow Terraform to destroy the instance. Unless this field is set to false in Terraform state, a terraform destroy or terraform apply that would delete the instance will fail."
  type        = bool
  default     = true
  }

variable "project_id" {
  description = "Project where the dataset and table are created"
  type        = string
}

variable "datasets" {
  description = "this is a test DS"
  default = []
  type = list(object({
        dataset_id = string
        friendly_name = string
        location = string
   }
  ))
}

variable "tables" {
  description = "A list of objects which include table_id, schema, clustering, time_partitioning, expiration_time and labels."
  default     = []
  type = list(object({
    table_id   = string,
    dataset_id = string, #added to test creating multi dataset
    schema     = string,
    clustering = list(string),
    deletion_protection=bool,
    time_partitioning = object({
      expiration_ms            = string,
      field                    = string,
      type                     = string,
      require_partition_filter = bool,
    }),
	range_partitioning = object({
      field = string,
      range = object({
        start    = string,
        end      = string,
        interval = string,
      }),
    }),
    expiration_time = string,
    labels          = map(string),
  }
  ))
}
