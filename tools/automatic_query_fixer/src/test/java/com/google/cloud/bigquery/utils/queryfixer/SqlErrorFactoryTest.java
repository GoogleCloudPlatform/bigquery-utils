package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.errors.*;
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
  public void getExpectKeywordButGotOthersError() {
    String message = "Syntax error: Expected keyword BY but got identifier \"bar\" at [1:34]";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof ExpectKeywordButGotOthersError);
    ExpectKeywordButGotOthersError error = (ExpectKeywordButGotOthersError) sqlError;
    assertEquals("BY", error.getExpectedKeyword());
  }

  @Test
  public void getIllegalInputCharacterError() {
    String message = "Syntax error: Illegal input character \"$\" at [1:78]";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof IllegalInputCharacterError);
    IllegalInputCharacterError error = (IllegalInputCharacterError) sqlError;
    assertEquals("$", error.getIllegalCharacter());
  }

  @Test
  public void getUnexpectedKeywordError() {
    String message = "Syntax error: Unexpected keyword HASH at [1:8]";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof UnexpectedKeywordError);
    UnexpectedKeywordError error = (UnexpectedKeywordError) sqlError;
    assertEquals("HASH", error.getKeyword());
  }

  @Test
  public void getDuplicateColumnsError() {
    String message =
        "Duplicate column names in the result are not supported. Found duplicate(s): status";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof DuplicateColumnsError);
    DuplicateColumnsError error = (DuplicateColumnsError) sqlError;
    assertEquals("status", error.getDuplicate());
  }

  @Test
  public void getColumnNotGroupedError() {
    String message =
        "SELECT list expression references column status which is neither grouped nor aggregated at [1:8]";
    BigQueryException exception = buildException(message);
    BigQuerySqlError sqlError = factory.getError(exception);
    assertTrue(sqlError instanceof ColumnNotGroupedError);
    ColumnNotGroupedError error = (ColumnNotGroupedError) sqlError;
    assertEquals("status", error.getMissingColumn());
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
