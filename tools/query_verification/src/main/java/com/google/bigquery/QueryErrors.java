package com.google.bigquery;

import com.google.auto.value.AutoValue;

/**
 * Value class for error statistics from multiple query results.
 */
@AutoValue
public abstract class QueryErrors {

    public abstract int totalQueries();
    public abstract int noErrors();
    public abstract int syntaxErrors();
    public abstract int semanticErrors();
    public abstract double successRate();

    public static QueryErrors create(int totalQueries, int noErrors, int syntaxErrors, int semanticErrors) {
        return new AutoValue_QueryErrors(totalQueries, noErrors, syntaxErrors, semanticErrors, noErrors * 100.0f / totalQueries);
    }

}
