# Get Started with UDF Unit Testing Using Dataform

## Let's get started!

This guide will show you how to run the Dataform example to unit test UDFs, as well as
how to unit test your own UDFs.

**Time to complete**: About 10-15 minutes

Click the **Start** button to move to the next step.

# Run the example to unit test UDFs

Now that you've already downloaded the example to Cloud Shell, you're ready to install Dataform and run the commands to get this example up and running. Follow these steps to run this example:

### 1. Install the Dataform CLI tool in the Cloud Shell terminal:
```bash
npm i -g @dataform/cli
```

### 2. Change into the `dataform_udf_unit_test` directory:
```bash
cd dataform/examples/dataform_udf_unit_test
```

### 3. Generate the Dataform credentials file by running the following:
```bash
dataform init-creds bigquery
```

**Note**: You will be prompted to select your dataset location (US, EU, or other), whether you would like to use Application Default Credentials (**recommended**) or a JSON key, and your billing project ID. Dataform will generate a file called `.df-credentials.json` that will contain your BigQuery project ID and the geographic location in which queries will run.

### 4. Execute the unit tests by running the following Dataform command:
```bash
dataform test
```

Congratulations, you successfully ran the example!

To learn how to modify this example to unit test your own UDFs, click **Next**.

# Run Your Own UDF Unit Tests

### 1. Create environment variables for the path to your Dataform directory and your BigQuery project ID:
Dataform project using these environment variables:
```bash
DATAFORM_DIR=<name-of-your-Dataform-project>
```
```bash
PROJECT_ID=<your-bigquery-project-id>
```

### 2. Create your Dataform project:
```bash
dataform init bigquery $DATAFORM_DIR --default-database $PROJECT_ID
```

### 3. While you’re still in the example directory, copy the `unit_test_utils.js` file to your Dataform project directory and change into your newly created Dataform project directory:
```bash
cp unit_test_utils.js $DATAFORM_DIR/includes/
cd $DATAFORM_DIR
```

### 4. Create your credentials file:
```bash
dataform init-creds bigquery
```

### 5. Create a new `test_cases.js` file:
```bash
echo "const {generate_udf_test} = unit_test_utils;" > $DATAFORM_DIR/definitions/test_cases.js
```
**Note**: This is the main file you'll be editing. You will be supplying the input(s) and expected outputs of your UDFs in this file.

### 6. Add a new invocation of the generate_udf_test() function for the UDF you want to test:
```
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
i. This function takes as arguments the string representing the name of the UDF you are going to test and an array of Javascript objects where each object holds the input(s) and expected output for a test case.

ii. You can either use the fully qualified UDF name (ex: bqutil.fn.url_parse) or just the UDF name (ex: url_parse).

iii. If your UDF accepts inputs of different data types, you will need to group your test cases by input data types with separate invocations of `generate_udf_test().`

### 7. Finally, run your tests to see if your UDF behaves as expected:
```bash
dataform test
```

## Congratulations 🎉

You have successfully ran the Dataform example and modified it to unit test your own UDFs!

### Want to Learn More?

Check out this [blog post](https://cloud.google.com/blog/) about unit testing your UDFs for your data warehouse migrations to BigQuery.
