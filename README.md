# BigQuery Utils

BigQuery is a serverless, highly-scalable, and cost-effective cloud data
warehouse with an in-memory BI Engine and machine learning built in. This
repository provides useful utilities to assist you in migration and usage of
BigQuery.

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2Fbigquery-utils.git)

## Getting Started

This repository is broken up into:

*   [Scripts](/scripts) - Python, Shell, & SQL scripts
    *   [billing](/scripts/billing) - Example queries over the GCP billing
        export
*   [Stored Procedures](/stored_procedures) - Example stored procedures
*   [UDFs](/udfs) - User-defined functions for common usage as well as migration
    *   [community](/udfs/community) - Community contributed user-defined
        functions
    *   [migration](/udfs/migration) - UDFs which mimic the behavior of
        proprietary functions in the following databases:
        *   [netezza](/udfs/migration/netezza)
        *   [oracle](/udfs/migration/oracle)
        *   [redshift](/udfs/migration/redshift)
        *   [teradata](/udfs/migration/teradata)
        *   [vertica](/udfs/migration/vertica)
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

All solutions within this repository are provided under the
[Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0) license. Please see
the [LICENSE](/LICENSE) file for more detailed terms and conditions.

## Disclaimer

This repository and its contents are not an official Google Product.
