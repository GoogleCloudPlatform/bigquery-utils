package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixOption;
import com.google.cloud.bigquery.utils.queryfixer.entity.FixResult;
import com.google.cloud.bigquery.utils.queryfixer.errors.BigQuerySqlError;
import com.google.cloud.bigquery.utils.queryfixer.errors.SqlErrorFactory;
import com.google.cloud.bigquery.utils.queryfixer.fixer.*;
import com.google.cloud.bigquery.utils.queryfixer.service.BigQueryService;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.CalciteTokenizer;
import com.google.cloud.bigquery.utils.queryfixer.tokenizer.QueryTokenProcessor;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class FixerTest {

  private static final String TABLE_2017 = "survey_2017";
  private static final String TABLE_2018 = "survey_2018";
  private static final String TABLE_2019 = "survey_2019";
  private static final String TABLE_2020 = "survey_2020";

  private SqlErrorFactory errorFactory;
  private FixerFactory fixerFactory;

  @Mock private BigQueryService bigQueryServiceMock;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    errorFactory = new SqlErrorFactory();
    QueryTokenProcessor tokenProcessor =
        new QueryTokenProcessor(new CalciteTokenizer(new BigQueryParserFactory()));
    fixerFactory = new FixerFactory(tokenProcessor, bigQueryServiceMock);
  }

  // TODO: More test cases are need to test different table name, like `a.b`.c, `a.b.c`, `a`.`b`.c,
  // and a.b.c.
  @Test
  public void fixTableNotFound() {
    setupBigQueryService_mockListTableNamesAndProjectId(
        ImmutableList.of(TABLE_2018, TABLE_2019, TABLE_2020), "");

    String query =
        String.format("Select max(foo) from %s group by bar limit 10", fullMockTable(TABLE_2017));
    String message =
        String.format(
            "Not found: Table bigquery-public-data:mock.%s was not found in location US",
            TABLE_2017);
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof TableNotFoundFixer);

    FixResult result = fixer.fix();
    assertEquals(2, result.getOptions().size());
    List<String> tables =
        result.getOptions().stream().map(FixOption::getAction).collect(Collectors.toList());

    assertThat(
        tables,
        contains(
            convertToAction(fullMockTable(TABLE_2018)),
            convertToAction(fullMockTable(TABLE_2019))));

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(22, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixTableNotFound_defaultProjectId() {
    setupBigQueryService_mockListTableNamesAndProjectId(
        ImmutableList.of(TABLE_2018, TABLE_2019, TABLE_2020), "bigquery-public-data");

    String query =
        String.format("Select max(foo) from %s group by bar limit 10", "`mock`." + TABLE_2017);
    String message =
        String.format(
            "Not found: Table bigquery-public-data:mock.%s was not found in location US",
            TABLE_2017);
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof TableNotFoundFixer);

    FixResult result = fixer.fix();
    assertEquals(2, result.getOptions().size());
    List<String> tables =
        result.getOptions().stream().map(FixOption::getAction).collect(Collectors.toList());

    assertThat(
        tables,
        contains(
            convertToAction(fullMockTable(TABLE_2018)),
            convertToAction(fullMockTable(TABLE_2019))));

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(22, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixUnrecognizedColumn() {
    String query = "SELECT state From `bigquery-public-data.austin_311.311_request` LIMIT 10";
    String message = "Unrecognized name: state; Did you mean status? at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof UnrecognizedColumnFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT status From `bigquery-public-data.austin_311.311_request` LIMIT 10",
        result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(8, result.getErrorPosition().getColumn());
  }

  // TODO: It is recommended to use parameterized tests to cover more complex cases.
  @Test
  public void fixFunctionNotFound() {
    String query =
        "SELECT CONCAT(\"prefix\", maxs(status), \"suffix\") From `bigquery-public-data.austin_311.311_request` LIMIT 10";
    String message = "Function not found: maxs; Did you mean max? at [1:25]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof FunctionNotFoundFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT CONCAT(\"prefix\", max(status), \"suffix\") From `bigquery-public-data.austin_311.311_request` LIMIT 10",
        result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(25, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixUnexpectedKeyword() {
    String query = "SELECT hash from `bigquery-public-data.crypto_bitcoin.blocks` LIMIT 1";
    String message = "Syntax error: Unexpected keyword HASH at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof UnexpectedKeywordFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT `hash` from `bigquery-public-data.crypto_bitcoin.blocks` LIMIT 1",
        result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(8, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixIllegalInputCharacter() {
    String query = "SELECT status$ FROM `bigquery-public-data.austin_311.311_request`  LIMIT 10";
    String message = "Syntax error: Illegal input character \"$\" at [1:14]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof IllegalInputCharacterFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT status FROM `bigquery-public-data.austin_311.311_request`  LIMIT 10",
        result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(14, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixExpectKeywordButGotOthers_useNearbyTokenFixer_fixFrom() {
    String expectedKeyword =
        "SELECT status FROM `bigquery-public-data.austin_311.311_request` LIMIT 10";
    mockDryRunQuery_noError(
        expectedKeyword, "Syntax error: Expected end of input but got X at [1:1]");

    String query = "SELECT status FORM `bigquery-public-data.austin_311.311_request` LIMIT 10";
    String message =
        "Syntax error: Expected end of input but got identifier `bigquery-public-data.austin_311.311_request` at [1:20]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NearbyTokenFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(expectedKeyword, result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(20, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixExpectKeywordButGotOthers_useNearbyTokenFixer_fixSelect() {
    String expectedKeyword = "SELECT status FROM `bigquery-public-data.austin_311.311_request`";
    mockDryRunQuery_noError(
        expectedKeyword, "Syntax error: Expected end of input but got X at [1:1]");

    String query = "SELCT status FROM `bigquery-public-data.austin_311.311_request`";
    String message = "Syntax error: Expected end of input but got identifier \"SELCT\" at [1:1]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NearbyTokenFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(expectedKeyword, result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(1, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixExpectKeywordButGotOthers_useNearbyTokenFixer_fail() {
    mockDryRunQuery_noError("", "Syntax error: Expected end of input but got X at [1:1]");

    String query =
        "Select status FROM `bigquery-public-data.austin_311.311_request` status != NULL";
    String message = "Syntax error: Expected end of input but got \"!=\" at [1:73]";

    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NearbyTokenFixer);

    FixResult result = fixer.fix();
    assertEquals(FixResult.Status.FAILURE, result.getStatus());
    assertNull(result.getOptions());
    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(73, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixExpectKeywordButGotOthers_useExpectKeywordFixer_insertKeyword() {
    String query = "SELECT status From `bigquery-public-data.austin_311.311_request` Group status";
    String message = "Syntax error: Expected keyword BY but got identifier \"status\" at [1:72]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof ExpectKeywordButGotOthersFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT status From `bigquery-public-data.austin_311.311_request` Group  BY status",
        result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(72, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixExpectKeywordButGotOthers_useExpectKeywordFixer_replaceKeyword() {
    String query =
        "SELECT status From `bigquery-public-data.austin_311.311_request` Group bye status";
    String message = "Syntax error: Expected keyword BY but got identifier \"bye\" at [1:72]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof ExpectKeywordButGotOthersFixer);

    FixResult result = fixer.fix();
    assertEquals(2, result.getOptions().size());
    assertEquals(
        "SELECT status From `bigquery-public-data.austin_311.311_request` Group BY status",
        result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(72, result.getErrorPosition().getColumn());
  }

  @Test
  public void fixDuplicateColumnsError1() {
    String query = "SELECT status, status FROM `bigquery-public-data.austin_311.311_request`";
    String message =
        "Duplicate column names in the result are not supported. Found duplicate(s): status";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof DuplicateColumnsFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT\n"
            + "  status AS status_1,\n"
            + "  status AS status_2\n"
            + "FROM\n"
            + "  `bigquery-public-data.austin_311.311_request`\n",
        result.getOptions().get(0).getFixedQuery());
  }

  @Test
  public void fixDuplicateColumns2() {
    String query =
        "SELECT status, a.status, b.status, concat(a.status, \"_suffix\") as status "
            + "FROM `bigquery-public-data.austin_311.311_request` a "
            + "inner join `bigquery-public-data.austin_311.311_request` b "
            + "on a.city = b.city LIMIT 1000";
    String message =
        "Duplicate column names in the result are not supported. Found duplicate(s): status";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof DuplicateColumnsFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT\n"
            + "  status AS status_1,\n"
            + "  a.status AS status_2,\n"
            + "  b.status AS status_3,\n"
            + "  concat(a.status, \"_suffix\") AS status_4\n"
            + "FROM\n"
            + "  `bigquery-public-data.austin_311.311_request` AS a\n"
            + "  INNER JOIN\n"
            + "  `bigquery-public-data.austin_311.311_request` AS b\n"
            + "  ON a.city = b.city\n"
            + "LIMIT 1000\n",
        result.getOptions().get(0).getFixedQuery());
  }

  @Test
  public void fixColumnNotGrouped_createGroupByClause() {
    String query =
        "SELECT status, max(unique_key) FROM `bigquery-public-data.austin_311.311_request` LIMIT 1000";
    String message =
        "SELECT list expression references column status which is neither grouped nor aggregated at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof ColumnNotGroupedFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT\n"
            + "  status,\n"
            + "  max(unique_key)\n"
            + "FROM\n"
            + "  `bigquery-public-data.austin_311.311_request`\n"
            + "GROUP BY status\n"
            + "LIMIT 1000\n",
        result.getOptions().get(0).getFixedQuery());
  }

  @Test
  public void fixColumnNotGrouped_updateGroupByClause() {
    String query =
        "SELECT status, max(unique_key) FROM `bigquery-public-data.austin_311.311_request` group by city LIMIT 1000";
    String message =
        "SELECT list expression references column status which is neither grouped nor aggregated at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof ColumnNotGroupedFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT\n"
            + "  status,\n"
            + "  max(unique_key)\n"
            + "FROM\n"
            + "  `bigquery-public-data.austin_311.311_request`\n"
            + "GROUP BY city, status\n"
            + "LIMIT 1000\n",
        result.getOptions().get(0).getFixedQuery());
  }

  @Test
  public void fixColumnNotGrouped_withKeywordLikeColumn() {
    String query =
        "SELECT `hash`,  mod(size, 100) as bucket FROM `bigquery-public-data.crypto_bitcoin.blocks` group by bucket, mod(number, 10) LIMIT 1000 ";
    String message =
        "SELECT list expression references column `hash` which is neither grouped nor aggregated at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof ColumnNotGroupedFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT\n"
            + "  `hash`,\n"
            + "  mod(size, 100) AS bucket\n"
            + "FROM\n"
            + "  `bigquery-public-data.crypto_bitcoin.blocks`\n"
            + "GROUP BY bucket, mod(number, 10), `hash`\n"
            + "LIMIT 1000\n",
        result.getOptions().get(0).getFixedQuery());
  }

  @Test
  public void noMatchingSignatureTest_legacyTypeCast_toString() {
    String query = "SELECT string(safe_cast(\"\" as bytes))";
    String message =
        "No matching signature for function STRING for argument types: BYTES. Supported signature: STRING(TIMESTAMP, [STRING]) at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NoMatchingSignatureFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT SAFE_CAST(safe_cast(\"\" as bytes) AS STRING)",
        result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(8, result.getErrorPosition().getColumn());
  }

  @Test
  public void noMatchingSignatureTest_legacyTypecast_intToTimestamp() {
    String query = "SELECT timestamp(1000)";
    String message =
        "No matching signature for function TIMESTAMP for argument types: INT64. Supported signatures: TIMESTAMP(STRING, [STRING]); TIMESTAMP(DATE, [STRING]); TIMESTAMP(DATETIME, [STRING]) at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NoMatchingSignatureFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals("SELECT TIMESTAMP_MICROS(1000)", result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(8, result.getErrorPosition().getColumn());
  }

  @Test
  public void noMatchingSignatureTest_intToDatetime() {
    String query = "SELECT datetime(1000)";
    String message =
        "No matching signature for function DATETIME for argument types: INT64. Supported signatures: DATETIME(INT64, INT64, INT64, INT64, INT64, INT64); DATETIME(DATE, TIME); DATETIME(TIMESTAMP, [STRING]); DATETIME(DATE) at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NoMatchingSignatureFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT DATETIME(TIMESTAMP_MICROS(1000))", result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(8, result.getErrorPosition().getColumn());
  }

  @Test
  public void noMatchingSignatureTest_ifNull() {
    String query = "SELECT IFNUll(1, \"23\")";
    String message =
        "No matching signature for function IFNULL for argument types: INT64, STRING. Supported signature: IFNULL(ANY, ANY) at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NoMatchingSignatureFixer);

    FixResult result = fixer.fix();
    assertEquals(1, result.getOptions().size());
    assertEquals(
        "SELECT IFNULL(1, SAFE_CAST(\"23\" AS INT64))", result.getOptions().get(0).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(8, result.getErrorPosition().getColumn());
  }

  @Test
  public void noMatchingSignatureTest_typeNotMatched() {
    String query = "select length(123)";
    String message =
        "No matching signature for function LENGTH for argument types: INT64. Supported signatures: LENGTH(STRING); LENGTH(BYTES) at [1:8]";
    BigQuerySqlError error = buildError(message);

    IFixer fixer = fixerFactory.getFixer(query, error);
    assertTrue(fixer instanceof NoMatchingSignatureFixer);

    FixResult result = fixer.fix();
    assertEquals(2, result.getOptions().size());
    assertEquals(
        "select LENGTH(SAFE_CAST(123 AS STRING))", result.getOptions().get(0).getFixedQuery());
    assertEquals(
        "select LENGTH(SAFE_CAST(123 AS BYTES))", result.getOptions().get(1).getFixedQuery());

    assertEquals(1, result.getErrorPosition().getRow());
    assertEquals(8, result.getErrorPosition().getColumn());
  }

  private String fullMockTable(String table) {
    return "bigquery-public-data.mock." + table;
  }

  private BigQuerySqlError buildError(String message) {
    BigQueryError bigQueryError = new BigQueryError("", "", message);
    BigQueryException exception = new BigQueryException(400, message, bigQueryError);
    return errorFactory.getError(exception);
  }

  private void setupBigQueryService_mockListTableNamesAndProjectId(
      List<String> tables, String projectId) {
    when(bigQueryServiceMock.listTableNames(any(String.class), any(String.class)))
        .thenReturn(tables);

    BigQueryOptions options = BigQueryOptions.newBuilder().setProjectId(projectId).build();
    when(bigQueryServiceMock.getBigQueryOptions()).thenReturn(options);
  }

  private String convertToAction(String identifier) {
    return String.format("Change to `%s`", identifier);
  }

  private void mockDryRunQuery_noError(String query, String defaultErrorForOthers) {
    when(bigQueryServiceMock.catchExceptionFromDryRun(any(String.class)))
        .thenReturn(buildException(defaultErrorForOthers));

    when(bigQueryServiceMock.catchExceptionFromDryRun(query)).thenReturn(null);
  }

  private BigQueryException buildException(String message) {
    BigQueryError bigQueryError = new BigQueryError("", "", message);
    return new BigQueryException(400, message, bigQueryError);
  }
}
