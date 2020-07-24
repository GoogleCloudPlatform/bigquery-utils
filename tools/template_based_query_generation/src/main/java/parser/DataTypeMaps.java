package parser;

import java.util.List;

/**
 * Helper class that contains all DataTypeMap(s) for JSON deserialization
 */
public class DataTypeMaps {
  /* contains all DataType mappings to PostgreSQL or BigQuery */
  public List<DataTypeMap> dataTypeMaps;

  public List<DataTypeMap> getDataTypeMaps() {
    return dataTypeMaps;
  }

  public void setDatatypes(List<DataTypeMap> dataTypeMaps) {
    this.dataTypeMaps = dataTypeMaps;
  }
}
