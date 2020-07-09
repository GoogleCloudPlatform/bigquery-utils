package com.google.bigquery;

import com.google.auto.value.AutoValue;

/**
 * Value class for queries. Contains the contents and path of the query file.
 */
@AutoValue
public abstract class QueryVerificationQuery {

    public abstract String query();
    public abstract String path();

    public static QueryVerificationQuery create(String query, String path) {
        return new AutoValue_QueryVerificationQuery(query, path);
    }

}