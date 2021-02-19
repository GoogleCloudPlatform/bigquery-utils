# Dataform Custom Assertions
Dataform is a platform to manage data in Big Query and other data warehouses and dataform perform the **T (Transformation)** in the **ELT** Pipeline. But before any transformaiton can be done, we need to make sure our input data is valid. Dataform has many built in data assertions such as uniqueKey, nonNull, and etc. But you can also customize your dataform assertions to meet individual's needs. This directory gives you a set of custom assertions that you can use in testing your data quailty in your project.
## Requirement 
In order to test and run the custom dataform assertions, the person has to have have: 
- A dataform project
- Credentials granting access to bigquery warehouse .df-credentials

## Usage
The custom assertions are in javascript files in the includes folder. To test them, simply refer to the function in your transformation query. For example, if a person wants to use the ```test_telephone_number_digits(colName)``` assertions in the ```phone_assertions.js``` file, the person only needs to refer in the config section of the transformation query like:

```
    type: ....,
    schema: .....,
    name: ....,
    assertions: {
        nonNull: ["phone_number"],
        rowConditions: 
        [
            phone_assertions.test_telephone_number_digits("phone_nunber")
        ]
    }
```
## Unit Testing Custom Assertions
Unit testing your custom assertions is important because it helps you safeguard your ELT pipeline. In this project, we want to demonstrate an easy way for you to unit test your custom assertions. The workflow is simple and as listed below:

* Create a ```test_[NAME_YOUR_TEST]_assertions.js``` file in the ```definitions/tests/``` folder if your custom row assertions are not included in the existing template.
* In ```test_[NAME_YOUR_TEST]_assertions.js``` change the code snippets ```const {[YOUR_CUSTOM_ASSERTION]} = [CUSTOMER_ASSERTION_FILE_NAME];``` and change the test name ```const test_name = "[YOUR_TEST_NAME]";```. 
* Add the testing data in the ```test_cases``` block with the following format ```"[INPUT]" : "[EXPECTED_OUTPUT]"```
* Finally supply your custom function name in the ```generatetest(...)``` function. 

Below is an example of the ```test_[NAME_YOUR_TEST]_assertions.js``` file:

```
const {generate_test} = unit_test_utils;
const {[YOUT_CUSTOM_ASSERTIONS]} = [CUSTOM_ASSERTION_FILE_NAME];
const test_name = "[YOUR_TEST_NAME]";
const test_cases = {
    /*
        Provide your own testing data following the structure
        <INPUT_TESTING_DATA> : "<EXPECTED OUTCOME>"
        For example, if a testing data has the <EXPECTED OUTCOME> to be TRUE,
        then the program will expect the custom data quality rules to also produce TRUE. 
        Otherwise it will show that the custom data quality rules failed. 
    */
    "[INPUT1]" : [EXPECTED_OUTPUT]"
    "[INPUT2]" : [EXPECTED_OUTPUT]"
    .
    .
    .
};
// The function below will generate the necessary SQL to run unit tests.
generate_test(test_name,
    test_cases,
    [YOUT_CUSTOM_ASSERTIONS]);
```

* Afterwards you can perform the unit test by running ```dataform test``` command. 
 
## Liscense
All solutions within this repository are provided under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. Please see the [LICENSE](https://www.apache.org/licenses/LICENSE-2.0) file for more detailed terms and conditions.