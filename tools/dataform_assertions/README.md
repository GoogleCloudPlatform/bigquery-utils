# Dataform Custom Assertions
Dataform is a platform to manage data in Big Query and other data warehouses and dataform perform the **T (Transformation)** in the **ELT** Pipeline. But before any transformaiton can be done, we need to make sure our input data is valid. Dataform has many built in data assertions such as uniqueKey, nonNull, and etc. But you can also customize your dataform assertions to meet individual's needs. This directory gives you a set of custom assertions that you can use in testing your data quailty in your project.
## Requirement 
In order to test and run the custom dataform assertions, the person has to have have: 
- A dataform project
- Credentials granting access to bigquery warehouse .df-credentials

## Usage
The custom assertions are in javascript files in the includes folder. To test them, simply refer to the function in your transformation query. For example, if a person wants to use the ```test_telephone_number_digits(colName)``` assertions in the ```phone_assertions.jd``` file, the person only needs to refer in the config section of the transformation query like:
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
## Workflow for adding new custom assertions
Below is the recommended workflow when adding new custom dataform assertions and testing the valididaty of the assertions
1. Browse the custom assertions files in ```includes/``` directory. If your custom assertion falls into one of the predefined custom assertions files, add your custom assertions to that file. Otherwise, please create a new file and add your custom assertion to it. 
2. When finished adding the javascript assertion file, checkout the unit test files in the ```tests/``` directory and add the unit test scripts to the appropriate test files. Please follow similiar syntax when adding unit test files. 
3. When finished adding unit test scripts, perform the command ```dataform run --tag [SELECT_TAG_TO_RUN]``` to check whether your custom assertions behaves accordingly. Note there are **3** custom tags to choose from ```date_unit_test```, ```personal_info_unit_test```, and ```phone_unit_test```. If you want to run all unit tests, use the tag ```all_test```

## Running Custom Row Assertions Unit Tests
Row custom assertions unit tests are grouped into 7 different tags: ```date_format_row_assertions_unit_test```, ```date_row_assertions_unit_test```, ```email_row_assertion_unit_test```, ```gender_row_assertion_unit_test```, ```marital_status_row_assertion_unit_test```, ```personal_info_row_assertion_unit_test```, and ```phone_row_assertions_unit_test```.
 To run individual unit tests, run with this command: ```dataform run --tags [SELECT_TAG_TO_RUN]```. For example, ```dataform run --tags date_row_assertions_unit_test```. If you want to run all 7 unit test together, run with command ```dataform run --tags all_test``` 

 ## Validating Custom Row Assertions Unit Tests
 After writing custom row assertions, we want to test and make sure our custom row assertions behaves as intended. Therefore, we have set up validation tests to validate our custom row assertions. The validation files live in the ```definitions/validation_test``` directory and similiar to custom row assertions, it has 7 validation tests. To perform a validation you simply run the command ```dataform run --tags [YOUR_VALIDATION_NAME]```. If the validation test does not return any ```assertion failed``` errors, then it means that your custom row assertions behaves correctly. Otherwise, your custom row assertions are erroneous.

## Liscense
All solutions within this repository are provided under the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. Please see the [LICENSE](https://www.apache.org/licenses/LICENSE-2.0) file for more detailed terms and conditions.