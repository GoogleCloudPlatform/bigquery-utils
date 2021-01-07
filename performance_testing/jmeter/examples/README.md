# JMeter example

This directory contains an example JMeter JMX file and sample queries
that use BigQuery public datasets.

# Requirements

* Apache JMeter
* A GCP project with BigQuery enabled

# Getting JMeter

Note that the scripts provided expect that you'll download
JMeter to this directory as described in the following
instructions.

You can download and unpack JMeter to this directory:

```
curl -L https://downloads.apache.org/jmeter/binaries/apache-jmeter-5.4.tgz | tar -xzf -
```

You will also need the Simba JDBC driver for BigQuery unpacked to JMeter's `lib` directory:

```
curl -LO https://storage.googleapis.com/simba-bq-release/jdbc/SimbaJDBCDriverforGoogleBigQuery42_1.2.12.1015.zip
unzip -n SimbaJDBCDriverforGoogleBigQuery42_1.2.12.1015.zip -d apache-jmeter-5.4/lib
```

Inspect the queries in the CSV files. Note that there's a query per line in the file, and
that they need to remain as a single line.


