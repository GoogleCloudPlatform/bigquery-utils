package com.google.bigquery;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Value class for query jobs. Contains the query, errors (if any), and the query results (if any).
 */
@AutoValue
public abstract class QueryJobResults {

    public abstract String statement();
    @Nullable public abstract QueryVerificationQuery query();
    @Nullable public abstract String error();
    @Nullable public abstract Set<List<Object>> results();
    @Nullable public abstract List<List<String>> rawResults();

    public static QueryJobResults create(String statement, QueryVerificationQuery query, String error, Set<List<Object>> results, List<List<String>> rawResults) {
        return new AutoValue_QueryJobResults(statement, query, error, results, rawResults);
    }

}