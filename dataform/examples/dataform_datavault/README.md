## Data Vault in BigQuery using Dataform
Data Vault is a recent modeling technique for data warehouses in addition to the traditional 3NF and Dimensional with the main goals of agility and auditability. 
Fundamentally, Data Vault moves relationships between the entities to separate tables so that entities can grow independently of each other. 
New relationships can be created or removed without any change in the existing tables.

## Example for its implementation on BigQuery using Dataform lib.
Dataform is an application to manage data pipelines for data warehouses like BigQuery, Snowflake, and Redshift. 
It enables data teams to build scalable, tested, SQL based data transformation pipelines using version control and engineering led best practices. It compiles hundreds of data models in under a second using SQLX. SQLX extends your existing SQL warehouse dialect to add features that support dependency management, testing, documentation and more.

## Steps to follow:

In order to test and run the dataform, please follow below steps:

1. Install Dataform using NPM to install Dataform
   - npm i -g @dataform/cli
2. Initialize a Dataform project and then create a new Dataform project

- dataform init bigquery dataform-demo --default-database $(gcloud config
  get-value project) --include-schedules

3. Set up authentication to BigQuery
    - Go to https://console.cloud.google.com/iam-admin/serviceaccounts and
      create a service account. Give this account the role of BigQuery Admin (so
      that Dataform can create new tables etc.). Then, download the JSON key to
      the project and upload the file to CloudShell.
    - Finally using terminal:
        - cd dataform-demo
        - dataform init-creds bigquery
    - Provide the path to the JSON key file when asked. Then make sure to add
      the key file to .gitignore so that you don’t check it in by mistake.
    - echo filename.json > .gitignore
    - git add -f .gitignore
4. Set up raw Sources and functions
    - Let’s start out by adding a definition for a source named
      definitions/sources/meter.sqlx. Use the CloudShell editor.
    - Function used in sql query can be placed under includes/hashing.js.
5. Compile and Dry Run
    - dataform compile
6. Run
    - dataform run

## Liscense
All solutions within this repository are provided under
the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. Please
see the [LICENSE](https://www.apache.org/licenses/LICENSE-2.0) file for more
detailed terms and conditions.

All solutions within this repository are provided under
Copyright 2022 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
