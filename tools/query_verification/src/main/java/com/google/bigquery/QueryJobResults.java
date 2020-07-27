package com.google.bigquery;

import com.google.auto.value.AutoValue;

/**
 * Value class for query jobs. Contains the query and the query results.
 * @param <T> Type for object storing results
 */
@AutoValue
public abstract class QueryJobResults<T> {

    public abstract String query();
    public abstract T results();

    public static <T> QueryJobResults<T> create(String query, T results) {
        return new AutoValue_QueryJobResults<T>(query, results);
    }

}
