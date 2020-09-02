package parser;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class that parses the main user config file
 */
public class User {
  /* the start of the Markov chain */
  String startFeature;

  /* the end of the Markov chain */
  String endFeature;

  /* the maximum number of columns in the resulting sample data */
  int numColumns;

  /* the number of rows in each table */
  int numRows;

  /* the number of queries to be generated */
  int numQueries;

  /* the number of tables in the database */
  int numTables;

  /* an indicator describing which dialects to output */
  Map<String, Boolean> dialectIndicators = new HashMap<>();

  public String getStartFeature() {
    return startFeature;
  }

  public void setStartFeature(String startFeature) {
    this.startFeature = startFeature;
  }

  public String getEndFeature() {
    return endFeature;
  }

  public void setEndFeature(String endFeature) {
    this.endFeature = endFeature;
  }

  public int getNumColumns() {
    return numColumns;
  }

  public void setNumColumns(int numColumns) {
    this.numColumns = numColumns;
  }

  public int getNumRows() {
    return this.numRows;
  }

  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }
  
  public int getNumQueries() {
    return numQueries;
  }

  public void setNumTables(int numTables) {
    this.numTables = numTables;
  }

  public int getNumTables() {
    return numTables;
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
}
