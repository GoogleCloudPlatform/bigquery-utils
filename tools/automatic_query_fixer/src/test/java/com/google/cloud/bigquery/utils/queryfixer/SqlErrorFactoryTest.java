package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.errors.BigQuerySqlError;
import com.google.cloud.bigquery.utils.queryfixer.errors.FunctionNotFoundError;
import com.google.cloud.bigquery.utils.queryfixer.errors.SqlErrorFactory;
import com.google.cloud.bigquery.utils.queryfixer.errors.TableNotFoundError;
import com.google.cloud.bigquery.utils.queryfixer.errors.UnrecognizedColumnError;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SqlErrorFactoryTest {

  private SqlErrorFactory factory;

  @Before
  public void setup() {
    factory = new SqlErrorFactory();
  }

  @Test
  public void getTableNotFoundError() {
    String message =
        "Not found: Table bigquery-public-data:austin_311.311_servce_requests was not found in location US";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof TableNotFoundError);
  }

  @Test
  public void getUnrecognizedColumnError() {
    String message = "Unrecognized name: statuses; Did you mean status? at [1:8]";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof UnrecognizedColumnError);
  }

  @Test
  public void getFunctionNotFoundError() {
    String message = "Function not found: sums; Did you mean sum? at [1:8]";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof FunctionNotFoundError);
  }

  @Test
  public void errorPosition() {
    String message = "Function not found: sums; Did you mean sum? at [12:34]";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertEquals(12, sqlError.getErrorPosition().getRow());
    assertEquals(34, sqlError.getErrorPosition().getColumn());
  }

  private BigQueryException buildException(String message) {
    BigQueryError bigQueryError = new BigQueryError("", "", message);
    return new BigQueryException(400, message, bigQueryError);
  }
}
