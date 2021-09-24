# Get Started with UDF Unit Testing Using Dataform

## Let's get started!

This guide will show you how to run the Dataform example to unit test UDFs, as
well as how to unit test your own UDFs.

**Time to complete**: About 10-15 minutes

Click the **Start** button to move to the next step.

## Run the example to unit test UDFs

Now that you've already downloaded the example to Cloud Shell and you're in the
example directory, you're ready to install Dataform and run the commands to get
this example up and running. Follow these steps to run this example:

### 1. Install the Dataform CLI tool in the Cloud Shell terminal:

```bash
npm i -g @dataform/cli && dataform install
```

### 2. Generate the Dataform credentials file by running the following:

```bash
dataform init-creds bigquery
```

> Note: You will be prompted to select the following:
>  * Your dataset location **(select ‘US’)**
>  * Your authentication method **(select Application Default Credentials)**.
>  * Your billing project ID (select the project in which you’ll run your queries).

### 3. Execute the unit tests by running the following Dataform command:

```bash
dataform test
```

Congratulations, you successfully ran the example!

To learn how to modify this example to unit test your own UDFs, click **Next**.

## Run Your Own UDF Unit Tests

### 1. While you’re still in the dataform_udf_unit_test directory, create environment variables for the name of your Dataform directory and your BigQuery project ID:

```bash
DATAFORM_DIR=<name-of-your-Dataform-project>
```

```bash
PROJECT_ID=<your-bigquery-project-id>
```

### 2. Create your Dataform project using the environment variables you set up in the previous step:

```bash
dataform init bigquery $DATAFORM_DIR --default-database $PROJECT_ID
```

### 3. While you’re still in the example directory, copy the `unit_test_utils.js` file to your Dataform project directory and change into your newly created Dataform project directory:

```bash
cp includes/unit_test_utils.js $DATAFORM_DIR/includes/
cd $DATAFORM_DIR
```

### 4. Create your credentials file:

```bash
dataform init-creds bigquery
```

### 5. Create a new `test_cases.js` file inside the `definitions/` directory:

```bash
echo "const {generate_udf_test} = unit_test_utils;" > definitions/test_cases.js
```

**Note**: This is the main file you'll be editing. You will be supplying the
input(s) and expected outputs of your UDFs in this file.

### 6. Edit your new `definitions/test_cases.js` file in the Cloud Editor (or shell) by adding a new invocation of the generate_udf_test() function for each UDF you want to test:

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

The `generate_udf_test()` function takes the following two positional arguments:

1. The first argument is a string representing the name of the UDF you will
   test. You can either use the fully qualified UDF name (ex:
   `bqutil.fn.url_parse`) or just the UDF name (ex: `url_parse`). If you provide
   just the UDF name, the function will use the `defaultDatabase` and
   `defaultSchema` values from your `dataform.json` file.

1. The second argument is an array of Javascript objects where each object holds
   the UDF positional inputs and expected output for a test case.

### 7. Finally, run your tests to see if your UDF behaves as expected:

```bash
dataform test
```

## Congratulations 🎉

You have successfully ran the Dataform example and modified it to unit test your
own UDFs!
