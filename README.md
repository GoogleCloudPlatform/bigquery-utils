# BigQuery Utils

BigQuery is a serverless, highly-scalable, and cost-effective cloud data
warehouse with an in-memory BI Engine and machine learning built in. This
repository provides useful utilities to assist you in migration and usage of
BigQuery.

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2Fbigquery-utils.git)

## Getting Started

This repository is broken up into:

*   [Dashboards](/dashboards) - Pre-built dashboards for common use cases
    *   [System Tables](/dashboards/system_tables) - Looker Studio Dashboards built on BigQuery's INFORMATION_SCHEMA metadata views to understand organization's slot and reservation utilization, job execution, and job errors.
*   [Notebooks](/notebooks) - Colab notebooks for various BigQuery related use cases.
    *   [BigQuery Frequent Items Sketches Demo](/notebooks/bigquery_frequent_items_sketches_demo.ipynb) - Calculating TopN file extensions in Git repos from public Github Dataset.
    *   [BigQuery KLL Sketches Demo](/notebooks/bigquery_kll_sketches_demo.ipynb) - Calculating Quantiles over user defined time-windows.
    *   [BigQuery Theta Sketches Demo](/notebooks/bigquery_theta_sketches_demo.ipynb) - Calculating Distinct User Logins over different months.
    *   [Geospatial](/dashboards/geospatial) - Colab notebooks for Geospatial Analytics.
*   [Performance Testing](/performance_testing) - Examples for doing performance testing
    *   [JMeter](/performance_testing/jmeter) - Examples for using JMeter to test BigQuery performance
*   [Scripts](/scripts) - Python, Shell, & SQL scripts
    *   [billing](/scripts/billing) - Example queries over the GCP billing
        export
    *   [optimization](/scripts/optimization) - Scripts to help identify areas for optimization in your BigQuery warehouse.
*   [Stored Procedures](/stored_procedures) - Example stored procedures
*   [Third Party](/third_party) - Relevant third party libraries for BigQuery
    *   [compilerworks](/third_party/compilerworks) - BigQuery UDFs which mimic the behavior of proprietary functions in other databases
*   [Tools](/tools) - Custom tooling for working with BigQuery
    *   [Cloud Functions](/tools/cloud_functions) - Cloud Functions to automate common use cases
*   [UDFs](/udfs) - User-defined functions for common usage as well as migration
    *   [community](/udfs/community) - Community contributed user-defined
        functions
    *   [datasketches](/udfs/datasketches) - UDFs deployed from the latest release of [Apache Datasketches for BigQuery](https://github.com/apache/datasketches-bigquery)
    *   [migration](/udfs/migration) - UDFs which mimic the behavior of
        proprietary functions in the following databases:
        *   [netezza](/udfs/migration/netezza)
        *   [oracle](/udfs/migration/oracle)
        *   [redshift](/udfs/migration/redshift)
        *   [snowflake](/udfs/migration/snowflake)
        *   [teradata](/udfs/migration/teradata)
        *   [vertica](/udfs/migration/vertica)
        *   [sqlserver](/udfs/migration/sqlserver)
*   [Views](/views) - Views over system tables such as audit logs or the
    `INFORMATION_SCHEMA`
    *   [query_audit](/views/audit/query_audit.sql) - View to simplify querying
        the audit logs which can be used to power dashboards
        ([example](https://codelabs.developers.google.com/codelabs/bigquery-pricing-workshop/#0)).


## Public UDFs

For more information on UDFs and using those provided in the repository with
BigQuery, see the [README](/udfs/README.md) in the [udfs](/udfs) folder.

## Contributing

See the contributing [instructions](/CONTRIBUTING.md) to get started
contributing.

To contribute UDFs to this repository, see the
[instructions](/udfs/CONTRIBUTING.md) in the [udfs](/udfs) folder.

## License

Except as otherwise noted, the solutions within this repository are provided under the
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. Please see
the [LICENSE](/LICENSE) file for more detailed terms and conditions.

## Disclaimer

This repository and its contents are not an official Google Product.
