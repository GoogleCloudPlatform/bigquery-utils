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
## How to modify the example for your own UDFs
1. Create environment variables for the path to your Dataform directory and your BigQuery project ID, then create your \
   Dataform project using these environment variables:
   ```bash
   DATAFORM_DIR=<name-of-your-Dataform-project>
   PROJECT_ID=<your-bigquery-project-id>
   dataform init bigquery $DATAFORM_DIR --default-database $PROJECT_ID
   ```
1. While you’re still in the example directory, copy the unit_test_utils.js file to your Dataform project directory. Then, \
   change into your newly created Dataform project directory and create your credentials file (.df-credentials.json):
   ```bash
   cp unit_test_utils.js $DATAFORM_DIR/includes/
   cd $DATAFORM_DIR
   dataform init-creds bigquery
   ```
1. Create a new test_cases.js  file:
   ```bash
   echo "const {generate_udf_test} = unit_test_utils;" > $DATAFORM_DIR/definitions/test_cases.js
   ```
1. Add a new invocation of the generate_udf_test() function for the UDF you want to test.
   1. This function takes as arguments the string representing the name of the UDF you are going to test and an array of\
      Javascript objects where each object holds the input(s) and expected output for a test case.
   2. You can either use the fully qualified UDF name (ex: bqutil.fn.url_parse) or just the UDF name (ex: url_parse). \
      If you provide just the UDF name, the function will use the defaultDatabase and defaultSchema values from your dataform.json file.
   3. If your UDF accepts inputs of different data types, you will need to group your test cases by input data types and\
      create a separate invocation of generate_udf_test case for each group of test cases. Refer to the json_typeof UDF\
      in the test_cases.js for an example of this implementation.
   ```bash
   generate_udf_test("YOUR_UDF_NAME", [  
      { // JS Object for test case #1
         inputs: [
            `TEST1_POSITIONAL_ARGUMENT_0`,
            `TEST1_POSITIONAL_ARGUMENT_1`],
         expected_output: `TEST1_EXPECTED_OUTPUT`
      },
      { // JS Object for test case #2
         inputs: [`TEST2_POSITIONAL_ARGUMENT_0`],
         expected_output: `TEST2_EXPECTED_OUTPUT`
      }
   ]);

   ```
1. Run your tests to see if your UDF behaves as expected:
    ```bash
    dataform test
    ```
