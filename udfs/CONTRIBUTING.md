# Contributing UDFs

Thank you for taking the time to contribute to this repository of user-defined
functions (UDFs) for BigQuery.

The following is a set of guidelines for contributing a UDF to this repository.

## UDF Contribution Guidelines

### Add your UDF

1.  Add your UDFs (**one** UDF per file) and a
    [Contributor License Agreement](#contributor-license-agreement) to the
    appropriate directory.
    *   If your function replicates logic from some other data warehouse UDF,
        place it in the relevant sub-directory in the
        [migration](/udfs/migration) directory. Otherwise, place it in the
        [community](/udfs/community) directory.
1.  Add test cases for your UDFs.
    *   Edit the `test_cases.yaml` file to include test inputs and expected
        outputs for the function. (take a look at the
        [community test_cases.yaml](community/test_cases.yaml) file as an
        example)
    *   Make sure test cases provide full coverage of the function's expected
        behavior. For example, if integers are the expected input, please
        provide test cases with the following inputs: negative numbers, zero,
        positive numbers, and null values.
1.  Describe what your UDF does.
    *   Edit the `README.md` in the associated sub-directory to include a
        description of the function and make sure your function is listed in
        alphabetical order amongst the other functions in the `README.md`.
    *   Make sure the same description is placed as a comment in your UDF file.

### Test your UDF

1.  Test your UDF locally using the test cases you added to the
    `test_cases.yaml` file. Please follow the instructions in the
    [Testing UDFs Locally section](#testing-udfs-locally) to automatically test
    all inputs for the expected outputs using the function.

### Submit a Pull Request

1.  Submit a pull request and we will review the code as soon as possible.
    Please see the section on [Code Reviews](#code-reviews) for more
    information.

Note: Your pull request, and any following commits, will trigger a testing
pipeline that will run unit tests on your submitted function as well as all the
other existing functions. This is done by a Cloud Build Trigger which runs a
Bash script. This Bash script unit tests the functions, running the contributed
UDFs in BigQuery with the given input to check that it results in the expected
output. If these tests pass, this will indicate to the reviewer that the
functions work as expected. So testing these functions locally before submitting
the pull request can ensure a successful review process.

## Testing UDFs Locally

Please follow these instructions to confirm that your test cases work as
expected.

1.  Change into the bigquery_utils top-level directory.

1.  Create a Python virtual environment and activate it:

    *   `python3 -m venv venv`
    *   `source venv/bin/activate`
    *   `pip install -r udfs/tests/requirements.txt`

1.  The test framework in this repo will create BigQuery datasets in your
    configured GCP project in order to test your UDF, and will delete them when
    finished testing. Authenticate using the Cloud SDK and set the GCP project
    in which you'll test your UDF(s):

    *   `gcloud auth login`
    *   `gcloud config set project YOUR_PROJECT_ID`

1.  Test your UDF by invoking the `run.sh` script and passing the name of the
    UDF in lower caps as an argument.

    *   `bash udfs/tests/run.sh url_parse`
        *   Note: If your UDF name exists in multiple directories, you can add
            the UDF's parent directory as a prefix \
            `bash udfs/tests/run.sh community_url_parse`

1.  Run all tests by invoking the `run.sh` script with no arguments

    *   `bash udfs/tests/run.sh`

1.  If all the tests pass, submit your pull request to proceed to the code
    review process.

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
