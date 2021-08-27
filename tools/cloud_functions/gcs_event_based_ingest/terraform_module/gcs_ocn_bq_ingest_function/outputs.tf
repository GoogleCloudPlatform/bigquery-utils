# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
output "cloud-function" {
  description = "instance of cloud function deployed by this module."
  value       = google_cloudfunctions_function.gcs_to_bq
}

output "data-ingester-sa" {
  description = "data ingester service account email created as cloud function identity"
  value       = module.data_ingester_service_account.email
}

output "input-bucket" {
  value = module.bucket.bucket.name
}

