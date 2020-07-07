package com.google.cloud.bigquery.utils.queryfixer;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;

import java.io.Reader;

/**
 * A factory to generate parsers. The fault generated parser is Babel Parser with BigQuery dialect.
 * */
public class BigQueryParserFactory {

  private final SqlParserImplFactory factory;
  private final Quoting quoting = Quoting.BACK_TICK;
  private final Casing unquotedCasing = Casing.TO_UPPER;
  private final Casing quotedCasing = Casing.UNCHANGED;
  private final SqlConformance conformance = SqlConformanceEnum.DEFAULT;
  private final SqlParser.Config parserConfig;

  /**
   * Default initialization with the Babel Parser factory.
   * */
  public BigQueryParserFactory() {
    factory = SqlBabelParserImpl.FACTORY;
    this.parserConfig = buildConfig();
  }

  /**
   * Initialization with customized factory
   * @param factory customized factory */
  public BigQueryParserFactory(SqlParserImplFactory factory) {
    this.factory = factory;
    this.parserConfig = buildConfig();
  }

  /**
   * Get the parser parsing the input query.
   * @param sql query to parse
   * @return a parser loaded with the query
   */
  public SqlParser getParser(String sql) {
    return getParser(new SourceStringReader(sql));
  }

  /**
   * Get the config of the parser factory
   * @return parser config
   */
  public SqlParser.Config getParserConfig() {
    return parserConfig;
  }

  protected SqlParser getParser(Reader source) {
    return SqlParser.create(source, parserConfig);
  }

  private SqlParser.Config buildConfig() {
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

