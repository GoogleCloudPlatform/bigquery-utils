# Contributing UDFs

Thank you for taking the time to contribute to this repository of user-defined
functions (UDFs) for BigQuery.

The following is a set of guidelines for contributing a UDF to this repository.

## UDF Contribution Guidelines

### Add your UDF

1. Add your UDFs (**one** UDF per file) to the appropriate directory.
    * If your function replicates logic from some other data warehouse UDF,
      place it in the relevant sub-directory in the
      [migration](/udfs/migration) directory. Otherwise, place it in the
      [community](/udfs/community) directory.
1. Add test cases for your UDFs.
    * Edit the `test_cases.js` file to include test inputs and expected outputs
      for the function. (take a look at the
      [community test_cases.js](community/test_cases.js) file as an example)
      > Note: If your UDF accepts inputs of different data types, you'll have to
      > create a separate generate_udf_test() invocation for each group of
      > inputs sharing identical data types. For example, the test cases in
      > [community test_cases.js](community/test_cases.js) for the
      > [int() UDF](community/int.sqlx) are split into three invocations of
      > generate_udf_test() since the test inputs can be grouped into the
      > following three groups of identical data types:
      >   * STRING
      >   * INT64
      >   * FLOAT64
    * Make sure test cases provide full coverage of the function's expected
      behavior. For example, if integers are the expected input, please provide
      test cases with the following inputs: negative numbers, zero, positive
      numbers, and null values.
1. Describe what your UDF does.
    * Edit the `README.md` in the associated sub-directory to include a
      description of the function and make sure your function is listed in
      alphabetical order amongst the other functions in the `README.md`.
    * Make sure the same description is placed as a comment in your UDF file.

### Test your UDF

The UDF testing framework in this repo will run on Cloud Build and perform the
following:

* Create BigQuery datasets for hosting the UDFs
* Deploy all UDFs in the BigQuery datasets
* Run all UDF unit tests in BigQuery
* Delete all BigQuery datasets and UDFs when finished

Please follow these instructions to run the testing framework which will confirm
that your UDFs behave as expected.

1. Change into the bigquery_utils [udfs/](./) directory:
   ```bash
   cd udfs/
   ```

1. Authenticate using the Cloud SDK and set the GCP project in which you'll test
   your UDF(s):

   ```bash 
   gcloud init
   ```

1. Enable the Cloud Build API and grant the default Cloud Build service account
   the BigQuery Job User role
   ```bash
   gcloud services enable cloudbuild.googleapis.com && \
   gcloud projects add-iam-policy-binding \
     $(gcloud config get-value project) \
     --member=serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")"@cloudbuild.gserviceaccount.com" \
     --role=roles/bigquery.user && \
   gcloud projects add-iam-policy-binding \
     $(gcloud config get-value project) \
     --member=serviceAccount:$(gcloud projects describe $(gcloud config get-value project) --format="value(projectNumber)")"@cloudbuild.gserviceaccount.com" \
     --role=roles/bigquery.dataEditor
   ```

1. Create a Google Cloud Storage Bucket to store JS libraries and test data in your project and run the UDF unit tests in Cloud Build by running the following:

   ```bash
   export JS_BUCKET=gs://YOUR_BUCKET/PATH/TO/STORE/JS_LIBS
   bash run_unit_tests.sh
   ```

1. If all the tests pass, submit your pull request to proceed to the code review
   process.

### Submit a Pull Request

1. Submit a pull request and we will review the code as soon as possible. Please
   see the section on [Code Reviews](#code-reviews) for more information.

> Note: Your pull request, and any following commits, will trigger a testing
> pipeline that will run unit tests on your submitted function as well as all
> the other existing functions. This is done by a Cloud Build Trigger which runs
> a Bash script. This Bash script unit tests the functions, running the
> contributed UDFs in BigQuery with the given input to check that it results in
> the expected output. If these tests pass, this will indicate to the reviewer
> that the functions work as expected. So testing these functions locally before
> submitting the pull request can ensure a successful review process.

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
[Google's Open Source Community Guidelines](https://opensource.google.com/conduct/)
.
