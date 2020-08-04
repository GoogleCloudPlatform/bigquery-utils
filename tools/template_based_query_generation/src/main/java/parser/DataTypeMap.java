package parser;

import data.DataType;

/**
 * Helper class that contains the PostgreSQL and BigQuery mappings for a datatype
 */
public class DataTypeMap {

  /* DataType in hidden language */
  DataType dataType;

  /* Equivalent PostgreSQL mapping to a datatype */
  String postgres;

  /* Equivalent BigQuery mapping to a datatype */
  String bigQuery;

  public DataType getDataType() {
    return dataType;
  }

  public void setDatatype(DataType dataType) {
    this.dataType = dataType;
  }

  public String getPostgres() {
    return postgres;
  }

  public void setPostgres(String postgres) {
    this.postgres = postgres;
  }

  public String getBigQuery() {
    return bigQuery;
  }

  public void setBigQuery(String bigQuery) {
    this.bigQuery = bigQuery;
  }
}
