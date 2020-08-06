package com.google.bigquery;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

/**
 * This class is where the logic for CalciteParser lives. Through instantiating the object, the
 * Calcite Parser can be used as a blackbox.
 */
public class CalciteParser implements Parser {
  private final SqlParser.Config config;

  public CalciteParser() {
    // can change the field here to change the type of Calcite Parser
    config = getParserConfig(SqlParserImpl.FACTORY);
  }

  /**
   * Parses the given query and returns a SqlString if successful and an exception otherwise.
   */
  @Override
  public String parseQuery(String query) throws SqlParseException {
    SqlParser myParser = SqlParser.create(query, config);
    SqlNode sqlNode = myParser.parseStmtList();
    return sqlNode.toSqlString(CalciteSqlDialect.DEFAULT).toString();
  }

  /**
   * Sets the configuration of the parser. Can change settings of the parser by changing code here.
   */
  public SqlParser.Config getParserConfig(SqlParserImplFactory factory) {
    return SqlParser.configBuilder()
        .setParserFactory(factory)
        .setQuotedCasing(Casing.UNCHANGED)
        .setUnquotedCasing(Casing.UNCHANGED)
        .setQuoting(Quoting.DOUBLE_QUOTE)
        .build();
  }
}
