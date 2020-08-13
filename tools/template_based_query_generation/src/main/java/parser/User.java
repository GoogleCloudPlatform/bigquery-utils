package parser;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class that parses the main user config file
 */
public class User {
  /* the start of the Markov chain */
  String start;

  /* the end of the Markov chain */
  String end;

  /* the maximum number of columns in the resulting sample data */
  int numColumns;

  /* the number of queries to be generated */
  int numQueries;

  /* an indicator describing which dialects to output */
  Map<String, Boolean> dialectIndicators = new HashMap<>();

  String bigQueryTable;

  public String getStart() {
    return start;
  }

  public void setStart(String start) {
    this.start = start;
  }

  public String getEnd() {
    return end;
  }

  public void setEnd(String end) {
    this.end = end;
  }

  public int getNumColumns() {
    return numColumns;
  }

  public void setNumColumns(int numColumns) {
    this.numColumns = numColumns;
  }

  public int getNumQueries() {
    return numQueries;
  }

  public void setNumQueries(int numQueries) {
    this.numQueries = numQueries;
  }

  public Map<String, Boolean> getDialectIndicators() {
    return dialectIndicators;
  }

  public void setDialectIndicators(Map<String, Boolean> dialectIndicators) {
    this.dialectIndicators = dialectIndicators;
  }

  public String getBigQueryTable() {
    return bigQueryTable;
  }

  public void setBigQueryTable(String bigQueryTable) {
    this.bigQueryTable = bigQueryTable;
  }
}
