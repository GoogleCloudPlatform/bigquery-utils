package com.google.cloud.bigquery.utils.autoqueryfixer;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.SourceStringReader;

import java.io.Reader;


public class BigQueryParserFactory {

  private final SqlParserImplFactory factory;
  private final Quoting quoting = Quoting.BACK_TICK;
  private final Casing unquotedCasing = Casing.TO_UPPER;
  private final Casing quotedCasing = Casing.UNCHANGED;
  private final SqlConformance conformance = SqlConformanceEnum.DEFAULT;
  private final SqlParser.Config parserConfig;

  public BigQueryParserFactory() {
    factory = SqlBabelParserImpl.FACTORY;
    this.parserConfig = buildConfig();
  }

  public BigQueryParserFactory(SqlParserImplFactory factory) {
    this.factory = factory;
    this.parserConfig = buildConfig();
  }

  public SqlParser getParser(String sql) {
    return getParser(new SourceStringReader(sql));
  }

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

