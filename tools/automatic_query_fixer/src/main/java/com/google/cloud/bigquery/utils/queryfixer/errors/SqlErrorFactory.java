package com.google.cloud.bigquery.utils.queryfixer.errors;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.utils.queryfixer.entity.Position;
import com.google.cloud.bigquery.utils.queryfixer.util.PatternMatcher;

import java.util.List;

/**
 * A factory to create {@link BigQuerySqlError} from {@link BigQueryException}. Currently, it
 * supports {@link TableNotFoundError}, {@link UnrecognizedColumnError}, and {@link
 * FunctionNotFoundError}.
 */
public class SqlErrorFactory {

  public static final String TableNotFoundRegex = "^Not found: Table (.*?) was not found";

  public static final String UnrecognizedNameRegex =
      "^Unrecognized name: (.*?)(; Did you mean (.*?)\\?)? at (.*?)$";

  public static final String FunctionNotFoundRegex =
      "^Function not found: (.*?)(; Did you mean (.*?)\\?)? at (.*?)$";

  public static final String UNEXPECTED_KEYWORD_REGEX =
      "^Syntax error: Unexpected keyword (.*?) at (.*?)$";

  public static final String ILLEGAL_INPUT_CHARACTER_REGEX =
      "^Syntax error: Illegal input character \"(.*?)\" at (.*?)$";

  public static final String EXPECT_KEYWORD_BUT_GOT_OTHERS_REGEX =
      "^Syntax error: Expected (.*?) but got (.*?) at (.*?)$";

  public static final String DUPLICATE_COLUMNS_REGEX =
      "^Duplicate column names in the result are not supported. Found duplicate\\(s\\): (.*?)$";

  public static final String COLUMN_NOT_GROUPED_REGEX =
      "^SELECT list expression references column \\`?(.*?)\\`? which is neither grouped nor aggregated at (.*?)$";

  /**
   * The method to convert {@link BigQueryException} to {@link BigQuerySqlError}. If the input
   * exception can not be resolved, a null pointer will be returned instead.
   *
   * @param exception the BigQueryException to resolve.
   * @return BigQuerySqlError object or null pointer.
   */
  public BigQuerySqlError getError(BigQueryException exception) {
    BigQuerySqlError error;

    if ((error = tryTableNotFoundError(exception)) != null) {
      return error;
    }

    if ((error = tryUnrecognizedNameError(exception)) != null) {
      return error;
    }

    if ((error = tryFunctionNotFoundError(exception)) != null) {
      return error;
    }

    if ((error = NoMatchingSignatureError.parse(exception)) != null) {
      return error;
    }

    if ((error = tryUnexpectedKeywordError(exception)) != null) {
      return error;
    }

    if ((error = tryIllegalInputCharacterError(exception)) != null) {
      return error;
    }

    if ((error = tryExpectKeywordButGotOthersError(exception)) != null) {
      return error;
    }

    if ((error = tryDuplicateColumnsError(exception)) != null) {
      return error;
    }

    if ((error = tryColumnNotGroupedError(exception)) != null) {
      return error;
    }

    return null;
  }

  /**
   * Try to convert the {@link BigQueryException} to {@link TableNotFoundError}. If it fails, a null
   * point will be returned.
   *
   * <p>The regex to extract information is `^Not found: Table (.*?) was not found`. Please see
   * {@link TableNotFoundError} for details on what information the regex extracts.
   *
   * @param exception BigQueryException
   * @return TableNotFoundError and null
   */
  private TableNotFoundError tryTableNotFoundError(BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(exception.getError().getMessage(), TableNotFoundRegex);
    if (contents == null) {
      return null;
    }

    // Here shows the index of matching places.
    // "^Not found: Table (0) was not found"
    // There is no need to check size, because the size of extracted substrings has been determined
    // by the pattern.
    String incorrectTable = contents.get(0);
    return new TableNotFoundError(incorrectTable, /*errPos= */ null, exception);
  }

  /**
   * Try to convert the {@link BigQueryException} to {@link UnrecognizedColumnError}. If it fails, a
   * null point will be returned.
   *
   * <p>The regex to extract information is `^Unrecognized name: (.*?)(; Did you mean (.*?)\?)? at
   * (.*?)`. Please see {@link UnrecognizedColumnError} for details on what information the regex
   * extracts.
   *
   * @param exception BigQueryException
   * @return TableNotFoundError and null
   */
  private UnrecognizedColumnError tryUnrecognizedNameError(BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(exception.getError().getMessage(), UnrecognizedNameRegex);
    if (contents == null) {
      return null;
    }

    // Here shows the index of matching places.
    // "^Unrecognized name: (0)(; Did you mean (2)\\?)? at (3)$"
    // There is no need to check size, because the size of extracted substrings has been determined
    // by the pattern.
    String unrecognizedName = contents.get(0);
    String suggestion = contents.get(2);
    String errPosStr = contents.get(3);
    Position errorPosition = PatternMatcher.extractPosition(errPosStr);

    return new UnrecognizedColumnError(unrecognizedName, errorPosition, suggestion, exception);
  }

  /**
   * Try to convert the {@link BigQueryException} to {@link FunctionNotFoundError}. If it fails, a
   * null point will be returned.
   *
   * <p>The regex to extract information is `^Function not found: (.*?)(; Did you mean (.*?)\\?)? at
   * (.*?)$`. Please see {@link FunctionNotFoundError} for details on what information the regex
   * extracts.
   *
   * @param exception BigQueryException
   * @return TableNotFoundError and null
   */
  private FunctionNotFoundError tryFunctionNotFoundError(BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(exception.getError().getMessage(), FunctionNotFoundRegex);
    if (contents == null) {
      return null;
    }
    // Here shows the index of matching places.
    // "^Function not found: (0)(; Did you mean (2)\\?)? at (3)$"
    // There is no need to check size, because the size of extracted substrings has been determined
    // by the pattern.
    String functionName = contents.get(0);
    String suggestion = contents.get(2);
    String errPosStr = contents.get(3);
    Position errorPosition = PatternMatcher.extractPosition(errPosStr);

    return new FunctionNotFoundError(functionName, errorPosition, suggestion, exception);
  }

  /**
   * Try to convert the {@link BigQueryException} to {@link UnexpectedKeywordError}. If it fails, a
   * null pointer will be returned.
   *
   * <p>The regex to extract information is "^Syntax error: Unexpected keyword (.*?) at (.*?)$",
   * where the first group is the unexpected keyword and the second one is the error position.
   *
   * @param exception BigQueryException
   * @return UnexpectedKeywordError or null
   */
  private UnexpectedKeywordError tryUnexpectedKeywordError(BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(exception.getError().getMessage(), UNEXPECTED_KEYWORD_REGEX);
    if (contents == null) {
      return null;
    }

    String keyword = contents.get(0);
    String errPosStr = contents.get(1);
    Position errorPosition = PatternMatcher.extractPosition(errPosStr);
    return new UnexpectedKeywordError(keyword, errorPosition, exception);
  }
  /**
   * Try to convert the {@link BigQueryException} to {@link IllegalInputCharacterError}. If it
   * fails, a null pointer will be returned.
   *
   * <p>The regex to extract information is "^Syntax error: Illegal input character \"(.*?)\" at
   * (.*?)$", where the first group is the illegal character and the second one is the error
   * position.
   *
   * @param exception BigQueryException
   * @return IllegalInputCharacterError or null
   */
  private IllegalInputCharacterError tryIllegalInputCharacterError(BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(exception.getError().getMessage(), ILLEGAL_INPUT_CHARACTER_REGEX);

    if (contents == null) {
      return null;
    }
    String illegalCharacter = contents.get(0);
    String errPosStr = contents.get(1);
    Position errorPosition = PatternMatcher.extractPosition(errPosStr);
    return new IllegalInputCharacterError(illegalCharacter, errorPosition, exception);
  }

  /**
   * Try to convert the {@link BigQueryException} to {@link ExpectKeywordButGotOthersError}. If it
   * fails, a null pointer will be returned.
   *
   * <p>The regex to extract information is "^Syntax error: Expected (.*?) but got (.*?) at (.*?)$",
   * where the first group is the expected token, the second one is the actual token, and third one
   * is the error position.
   *
   * @param exception BigQueryException
   * @return ExpectKeywordButGotOthersError or null
   */
  private ExpectKeywordButGotOthersError tryExpectKeywordButGotOthersError(
      BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(
            exception.getError().getMessage(), EXPECT_KEYWORD_BUT_GOT_OTHERS_REGEX);

    if (contents == null) {
      return null;
    }

    String expectedKeyword = extractExpectedKeyword(contents.get(0));
    String errPosStr = contents.get(2);
    Position errorPosition = PatternMatcher.extractPosition(errPosStr);
    return new ExpectKeywordButGotOthersError(expectedKeyword, errorPosition, exception);
  }

  private String extractExpectedKeyword(String expectedToken) {
    List<String> contents = PatternMatcher.extract(expectedToken, /*regex=*/ "^keyword (.*?)$");
    if (contents == null) {
      return ExpectKeywordButGotOthersError.END_OF_INPUT;
    }
    return contents.get(0);
  }

  /**
   * Try to convert the {@link BigQueryException} to {@link DuplicateColumnsError}. If it fails, a
   * null pointer will be returned.
   *
   * <p>The regex to extract information is "^Duplicate column names in the result are not
   * supported. Found duplicate\(s\): (.*?)$", where the group is the duplicate column.
   *
   * @param exception BigQueryException
   * @return DuplicateColumnsError or null
   */
  private DuplicateColumnsError tryDuplicateColumnsError(BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(exception.getError().getMessage(), DUPLICATE_COLUMNS_REGEX);

    if (contents == null) {
      return null;
    }

    String duplicate = contents.get(0);
    return new DuplicateColumnsError(duplicate, exception);
  }

  /**
   * Try to convert the {@link BigQueryException} to {@link ColumnNotGroupedError}. If it fails, a
   * null pointer will be returned.
   *
   * <p>The regex to extract information is "^SELECT list expression references column \`?(.*?)\`?
   * which is neither grouped nor aggregated at (.*?)$", where the first group is the missing
   * (ungrouped) column and the second group is the error position.
   *
   * @param exception BigQueryException
   * @return ColumnNotGroupedError or null
   */
  private ColumnNotGroupedError tryColumnNotGroupedError(BigQueryException exception) {
    List<String> contents =
        PatternMatcher.extract(exception.getError().getMessage(), COLUMN_NOT_GROUPED_REGEX);

    if (contents == null) {
      return null;
    }

    String missingColumn = contents.get(0);
    String errPosStr = contents.get(1);
    Position errorPosition = PatternMatcher.extractPosition(errPosStr);
    return new ColumnNotGroupedError(missingColumn, errorPosition, exception);
  }
}
