package com.google.cloud.bigquery.utils.queryfixer;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;
import org.apache.commons.lang3.reflect.FieldUtils;

import com.google.cloud.bigquery.utils.queryfixer.exception.ParserCreationException;
import com.google.common.base.Preconditions;

import java.io.Reader;

/**
 * A factory to generate parsers. The default generated parser is a Babel Parser with BigQuery
 * dialect.
 */
public class BigQueryParserFactory {

  private static final Quoting quoting = Quoting.BACK_TICK;
  private static final Casing quotedCasing = Casing.UNCHANGED;
  private static final SqlConformance conformance = SqlConformanceEnum.DEFAULT;
  private final SqlParser.Config parserConfig;
  private static final Casing unquotedCasing = Casing.UNCHANGED;

  /** Default initialization with the Babel Parser factory. */
  public BigQueryParserFactory() {
    this(SqlBabelParserImpl.FACTORY);
  }

  /**
   * Initialization with customized factory
   *
   * @param factory customized factory
   */
  public BigQueryParserFactory(SqlParserImplFactory factory) {
    this.parserConfig = buildConfig(factory);
  }

  /**
   * Get the parser parsing the input query.
   *
   * @param query query to parse
   * @return a parser loaded with the query
   */
  public SqlParser getParser(String query) {
    return getParser(new SourceStringReader(query));
  }

  /**
   * Get the implementation of a parser parsing a query. The implementation provides functions to
   * tokenize the query.
   *
   * @param query the query fed to the parser implementation.
   * @return SqlBabelParserImpl the parser implementation
   */
  public SqlBabelParserImpl getBabelParserImpl(String query) {
    Preconditions.checkNotNull(query, "The input query should not be null.");

    Object parserImpl;
    try {
      parserImpl =
          FieldUtils.readField(
              getParser(query), /* fieldName= */ "parser", /* forceAccess= */ true);
    } catch (IllegalAccessException e) {
      throw new ParserCreationException(
          "Unable to extract the parserImpl from the generated parser.");
    }

    if (!(parserImpl instanceof SqlBabelParserImpl)) {
      throw new ParserCreationException(
          "This factory does not produce Babel Parser. Use the BigQueryParserFactory with default initialization instead.");
    }
    return (SqlBabelParserImpl) parserImpl;
  }

  protected SqlParser getParser(Reader source) {
    return SqlParser.create(source, parserConfig);
  }

  private SqlParser.Config buildConfig(SqlParserImplFactory factory) {
    final SqlParser.ConfigBuilder configBuilder =
        SqlParser.configBuilder()
            .setParserFactory(factory)
            .setQuoting(quoting)
            .setUnquotedCasing(unquotedCasing)
            .setQuotedCasing(quotedCasing)
            .setConformance(conformance);
    return configBuilder.build();
  }
}
