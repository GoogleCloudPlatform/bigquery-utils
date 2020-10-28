package com.google.bigquery;

import com.google.auto.value.AutoValue;

import java.util.List;

/**
 * Value class for result differences with results classified as extra or missing from migrated results.
 */
@AutoValue
abstract class ResultDifferences {

    public abstract List<List<String>> extraResults();
    public abstract List<List<String>> missingResults();

    public static ResultDifferences create(List<List<String>> extraResults, List<List<String>> missingResults) {
        return new AutoValue_ResultDifferences(extraResults, missingResults);
    }

}
