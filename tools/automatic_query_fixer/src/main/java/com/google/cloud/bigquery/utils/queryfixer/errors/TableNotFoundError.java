package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;

import lombok.Getter;

/**
 * A class to represent the "Table Not Found" errors from BigQuery. The errors are presented in this
 * form: "Not found: Table [TableName] was not found", where [TableName] is the incorrect table
 * name.
 */
@Getter
public class TableNotFoundError extends BigQuerySemanticError {

  private final String tableName;

  public TableNotFoundError(String tableName, Position errorPosition, BigQueryException errorSource) {
    super(errorPosition, errorSource);
    this.tableName = tableName;
  }
}
