# Contributing UDFs

Thank you for taking the time to contribute to this repository of user-defined functions (UDFs) for BigQuery.

The following is a set of guidelines for contributing a UDF to this repository.

## Pull Request Process

1. In order to contribute a UDF to this repository, please include these changes in a pull request:
    * Add a SQL file containing the UDF and a Contributor License Agreement (see the below [section](#contributor-license-agreement) for more information) to the appropriate directory. If this is a function replicating something in a data warehouse, choose the associated sub-directory in the [migration](/udfs/migration) directory. Otherwise, choose the [community](/udfs/community) directory.
    * Edit the `test_cases.yaml` file to include test inputs and expected outputs for the function. Make sure test cases provide full coverage of the function's expected behavior. For example, if integers are expected input, please provide test cases with the following inputs: negative numbers, zero, positive numbers, and null values.
    * Edit the `README.md` in the associated sub-directory to include a description of the function. Make sure your function is listed in alphabetical order amongst the other functions in the `README.md`. 
2. Test your UDF locally using the test cases you added to the `test_cases.yaml` file. Please follow the instructions in the [Testing UDFs Locally section](#testing-udfs-locally) to automatically test all inputs for the expected outputs using the function.
3. Submit a pull request and we will review the code as soon as possible. Please see the section on [Code Reviews](#code-reviews) for more information.

## Testing UDFs Locally

Please follow these instructions to confirm that the test cases being provided in your pull request work as expected.

0. Change into the bigquery_utils top-level directory.
1. Create a Python virtual environment.
2. Run this command to install all requirements: `python3 -m pip install -r udfs/tests/requirements.txt`
3. Temporarily create a separate directory at the same level where your function currently exists. Place the function inside this directory.
4. Edit the test_DIRECTORY_udf.py file's UDFS_DIR_PATH variable, placing in the path of your temporarily created directory containing the SQL file with your UDF.
5. Run this command after replacing DIRECTORY with the correct directory (community or the EDW named directory): `#python3 -m nose2 -c udfs/tests/unittest.cfg test_DIRECTORY_udf` 
6. If all the tests pass, you may submit the pull request. Delete the temporary directory and place the file back at the correct level. If the tests didn't pass, figure out what went wrong and try again.

This script performs unit testing on your one function. A continuous integration pipeline triggered by a pull request to the repository will test your submitted function as well as all the existing functions. This is done by a Cloud Build Trigger which runs a Bash script. This Bash script unit tests the functions, running the contributed UDFs in BigQuery with the given input to check that it results in the expected output. If these tests pass, this will indicate to the reviewer that the functions work as expected. So testing these functions locally before submitting the pull request can ensure a successful review process.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).
