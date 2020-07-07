package com.google.bigquery;

import com.google.auto.value.AutoValue;

/**
 * Value class for schema. Contains the contents and path of the schema file.
 */
@AutoValue
public abstract class QueryVerificationSchema {

    public abstract String schema();
    public abstract String path();

    public static QueryVerificationSchema create(String schema, String path) {
        return new AutoValue_QueryVerificationSchema(schema, path);
    }

}