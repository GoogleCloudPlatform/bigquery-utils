
#!/usr/bin/python3.7
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================

""" Python Script to create a dataset protected with CMEK stored in Cloud KMS

Make sure to meet the prerequisites. Ref : https://cloud.google.com/bigquery/docs/customer-managed-encryption#before_you_begin
   a) Creating required Cloud KMS resources
   b) Providing required IAM permissions to the service account used to run this script.
   c) Use the latest python client google-cloud-bigquery 1.24.0 [https://pypi.org/project/google-cloud-bigquery] as the default_encryption_configuration attribute was added post version 1.22.0
"""
from google.cloud import bigquery
from google.oauth2 import service_account
import logging
import traceback
import sys

# Create Service account credential file using the ref :   https://cloud.google.com/iam/docs/creating-managing-service-account-keys#iam-service-account-keys-create-console
# Needs to be replaced with user's service account credential file
_CRED_FILE = "credentials/service_account.json"
Logger = None
BigqueryClient = None

def init_logger():
    """Initializing the logger global variable for stdout"""
    global Logger
    logger_name = "bqclient"
    Logger = logging.getLogger(logger_name)
    logging.basicConfig(level=logging.INFO)




def init_bigquery_client():
    """Initializing Bigquery Client using input credential file for authentication"""
    global BigqueryClient
    cloud_platform_scope = "https://www.googleapis.com/auth/cloud-platform"

    # When your application needs access to user data, it asks Google for a particular scope of access.
    # Access tokens are associated with a scope, which limits the token's access
    # References: https://developers.google.com/drive/api/v2/about-auth
    #             https://cloud.google.com/bigquery/docs/authorization
    credentials = service_account.Credentials.from_service_account_file(
        _CRED_FILE,
        scopes=[cloud_platform_scope],
    )
    BigqueryClient = bigquery.Client(credentials=credentials,project=credentials.project_id)
# Api Ref : 
# Creating dataset : https://googleapis.dev/python/bigquery/1.24.0/generated/google.cloud.bigquery.client.Client.html#google.cloud.bigquery.client.Client.create_dataset

# Setting encryption for dataset : https://googleapis.dev/python/bigquery/1.24.0/generated/google.cloud.bigquery.dataset.Dataset.html#google.cloud.bigquery.dataset.Dataset.default_encryption_configuration

# Doc: https://cloud.google.com/bigquery/docs/customer-managed-encryption
def create_dataset_with_cmek(kms_project_id, dest_dataset_id, kms_location, kms_key_ring_id, kms_key_id):
    """
    Creates a BigQuery dataset with CMEK

    Attributes :
        kms_project_id : the project ID of the project running Cloud KMS
        dest_dataset_id : the fully-qualified dataset id i.e "your_project_id.your_dataset_id" where "you_project_id" in the destination project id where dataset with name "your_dataset_id" will be created
        kms_location : location of the cloud KMS resource. Ref: https://cloud.google.com/kms/docs/object-hierarchy#location
        kms_key_ring_id : keyring id of the cloud KMS resource. Ref : https://cloud.google.com/kms/docs/object-hierarchy#key_ring
        kms_key_id : the cryptographic key used to encrypt the dataset. Ref : https://cloud.google.com/kms/docs/object-hierarchy#key

    Returns:
        0 in case of success, nonzero on failure.
      """
    # Setting default exit code
    exit_code = 0



    try :
        # Ref : https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.dataset.DatasetReference.html#google.cloud.bigquery.dataset.DatasetReference
        dataset_ref = bigquery.dataset.DatasetReference.from_string(dest_dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        # Set the encryption key to use for the dataset.
        # Ref : https://cloud.google.com/kms/docs/object-hierarchy#key_resource_id
        kms_key_name = "projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}".format(
            kms_project_id, kms_location, kms_key_ring_id, kms_key_id
        )
        dataset.default_encryption_configuration = bigquery.EncryptionConfiguration(
            kms_key_name=kms_key_name
        )
        dataset = BigqueryClient.create_dataset(dataset)  # API request
        Logger.info("Successfully created BQ dataset : {}".format(dataset.__dict__))

    except Exception as error:
        exit_code = 1
        Logger.error("Failed to create CMEK protected bigquery dataset : {}".format( traceback.format_exc()))

    return exit_code

if __name__ == '__main__':
    init_logger()
    init_bigquery_client()
    # Provide custom values for each of the function attributes. See function definition for details about the attributes
    exit_code = create_dataset_with_cmek("nikunjbhartia-sce-testenv", "nikunjbhartia-test-clients.hsbc_cmek_dataset_poc_v9" , "us", "hsbc-test-keyring" , "hsbc-bq-dataset-key")
    sys.exit(exit_code)



