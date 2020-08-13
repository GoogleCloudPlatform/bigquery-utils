package com.google.bigquery;

import com.google.auto.value.AutoValue;

import java.util.List;

/**
 * Value class for result differences with results classified as extra or missing from migrated results.
 */
@AutoValue
abstract class ResultDifferences {

    public abstract List<List<Object>> extraResults();
    public abstract List<List<Object>> missingResults();

    public static ResultDifferences create(List<List<Object>> extraResults, List<List<Object>> missingResults) {
        return new AutoValue_ResultDifferences(extraResults, missingResults);
    }

}
