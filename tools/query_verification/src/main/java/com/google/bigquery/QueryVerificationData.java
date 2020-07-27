package com.google.bigquery;

import com.google.auto.value.AutoValue;

/**
 * Value class for table data. Contains the table name and file of the data.
 */
@AutoValue
public abstract class QueryVerificationData {

    public abstract String datasetName();
    public abstract String tableName();
    public abstract String path();
    public abstract String contents();

    public static QueryVerificationData create(String datasetName, String tableName, String path, String contents) {
        return new AutoValue_QueryVerificationData(datasetName, tableName, path, contents);
    }

}