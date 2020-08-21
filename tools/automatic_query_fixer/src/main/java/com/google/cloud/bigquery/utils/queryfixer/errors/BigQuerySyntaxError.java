package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;

/**
 * A class to represent the syntax errors of BigQuery.
 */
public abstract class BigQuerySyntaxError extends BigQuerySqlError {

    public BigQuerySyntaxError(Position errorPosition, BigQueryException errorSource) {
        super(errorPosition, errorSource);
    }
}
