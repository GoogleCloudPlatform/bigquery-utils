# BigQuery Simba JDBC Driver Quick-start

BigQuery provides a JDBC driver to help users interact with BigQuery's SQL query engine using existing tooling and applications. 

The latest JDBC driver and its corresponding documentation can be downloaded from the [Google Cloud documentation](https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers).

This Java project intends to be a quick-start code sample to help users get up and running with querying BigQuery via a JDBC application. It should not be assumed production-ready, and is not an officially supported product. This sample was developed with release 1.5.2.1005.


## Code structure
The following files are provided:
* src/main/java/com/example/BQSimbaExample.java
  * Main example showing how to authenticate, configure, and execute queries.
* src/main/java/com/example/OAuthUserConfig.java
  * OAuth user config object and builder.
* src/main/java/com/example/OAuthUserType.java
  * Enum for available OAuth user types.
* pom.xml
  * Project configuration and dependencies.

## Setup
1. Download and extract the JDBC driver from the [BigQuery JDBC documentation](https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers#current_jdbc_driver). The driver is not available via Maven (as of June 2024).   

2. The extract will contain `GoogleBigQueryJDBC42.jar` along with all its dependencies. Since the BQ driver itself is not available on Maven, it must be installed locally. This can be done using the command below. The remaining jars/dependencies can be found in the public Maven repo and are covered by a handful of top-level dependencies - these are included in the provided `pom.xml`. The versions may need to be updated to match those in your downloaded driver zip.
    ```
    mvn install:install-file \
         -Dfile=/Users/user/Downloads/SimbaJDBCDriverforGoogleBigQuery42_1.5.2.1005/GoogleBigQueryJDBC42.jar \
         -DgroupId=com.simba.googlebigquery \
         -DartifactId=googlebigquery-jdbc42 \
         -Dversion=1.5.2.1005 \
         -Dpackaging=jar \
         -DgeneratePom=true
   
   mvn clean package
   ```

3. Configure the required variables in BQSimbaExample.main:
   1. Set the default project vars (ie. `projectId`, `additionalDataProjects`, ...).
   2. Configure the auth variables to use a service account (ie. `serviceAccountEmail`, `serviceAccountFile`). Take a look at the driver documentation along with the `OAuthUserType` and `OAuthUserConfig` classes to see how you can authenticate as a type other than Service Account.   

4. Execute `BQSimbaExample.main`.