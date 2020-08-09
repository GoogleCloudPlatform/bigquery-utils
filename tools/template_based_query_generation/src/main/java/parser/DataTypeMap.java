package parser;

import data.DataType;

import java.util.Map;

/**
 * Helper class that contains the PostgreSQL and BigQuery mappings for a datatype
 */
public class DataTypeMap {

  /* DataType in hidden language */
  DataType dataType;

  /* List of dialect maps to each keyword */
  private Map<String, String> dialectMap;

  public DataType getDataType() {
    return dataType;
  }

  public void setDatatype(DataType dataType) {
    this.dataType = dataType;
  }

  public Map<String, String> getDialectMap() {
    return this.dialectMap;
  }

  public void setDialectMap(Map<String, String> dialectMap) {
    this.dialectMap = dialectMap;
  }
}
