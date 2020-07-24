package data;

import java.util.HashMap;

/**
 * class representing a data table
 * contains data table and schema
 */
public class Table {
  private String name;
  private HashMap<String, DataType> schema;

  /**
   * constructs empty table from table name
   * @param name
   */
  public Table(String name) {
    this.name = name;
    this.schema = new HashMap<String, DataType>();
  }

  /**
   * adds column to table
   * @param columnName
   * @param type
   */
  public void addColumn(String columnName, DataType type) {
    this.schema.put(columnName, type);
  }

  public HashMap<String, DataType> getSchema() {
    return this.schema;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }


}
