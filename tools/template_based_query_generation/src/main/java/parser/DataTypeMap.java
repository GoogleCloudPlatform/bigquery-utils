package parser;

import data.DataType;

import java.util.List;

/**
 * Helper class that contains the PostgreSQL and BigQuery mappings for a datatype
 */
public class DataTypeMap {

  /* DataType in hidden language */
  DataType dataType;

  /* List of dialect maps to each keyword */
  private List<DialectMap> dialectMaps;

  public DataType getDataType() {
    return dataType;
  }

  public void setDatatype(DataType dataType) {
    this.dataType = dataType;
  }

  public List<DialectMap> getDialectMaps() {
    return this.dialectMaps;
  }

  public void setDialectMaps(List<DialectMap> dialectMaps) {
    this.dialectMaps = dialectMaps;
  }
}
