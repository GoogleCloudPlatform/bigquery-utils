# Dataform UDF Unit Testing Example

## How to run this example

1. Clone this repo (click below to clone in Cloud Shell) \
   [![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2Fbigquery-utils.git&cloudshell_workspace=dataform/examples/dataform_assertion_unit_test&cloudshell_tutorial=README.md)
1. Install Dataform CLI tool
    ```bash
    npm i -g @dataform/cli
    ```
1. Change into this directory
    ```bash
    cd dataform/examples/dataform_udf_unit_test
    ```
1. Generate the Dataform credentials file by running the following:
   ```bash
   dataform init-creds bigquery
   ```
1. Execute the unit tests by running the following:
    ```bash
    dataform test
    ```
## How to modify for your own UDFs