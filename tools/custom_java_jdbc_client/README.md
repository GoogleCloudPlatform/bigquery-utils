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
   1. Set default values for projects and datasets (ie. `projectId`, `additionalDataProjects`, `defaultDataset`). These are fallback values if they are not specified using command line args.
   2. The example uses Application Default Credentials for GCP authentication. Refer to the [ADC documentation](https://cloud.google.com/docs/authentication/provide-credentials-adc) to set this up. Take a look at the JDBC driver documentation, along with the `OAuthUserType` and `OAuthUserConfig` classes, for other authentication options.  

4. Run `mvn clean package` to build the jar. Then, execute `BQSimbaExample` with the following args:
   1. Required: query, eg. `query="SELECT id, name FROM mydataset.mytable WHERE id > 100;"`. 
   2. Optional: `projectId`, `additionalDataProjects`, `defaultDataset`. If not specified, these will use the defaults set in step 3i above.
   3. Example command using `mvn exec`:
   ```mvn exec:java -Dexec.args="query=\"SELECT id, name FROM dataset1.table1 WHERE id='2'\" projectId=pso-amex-data-platform additionalDataProjects=pso-amex-data-platform defaultDataset=dataset1"```.
