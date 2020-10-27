package data;

import org.apache.commons.lang3.tuple.MutablePair;
import parser.Utils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


/**
 * class representing a data table
 * contains data table and schema
 */
public class Table {

  private String name;
  private int numRows;
  private ArrayList<MutablePair<String, DataType>> schema;

  /**
   * constructs empty table from table name
   * @param name
   */
  public Table(String name, int numRows) {
    this.name = name;
    this.numRows = numRows;
    this.schema = new ArrayList<>();
  }

  /**
   * adds column to table
   * @param columnName
   * @param type
   */
  public void addColumn(String columnName, DataType type) {
    this.schema.add(new MutablePair(columnName, type));
  }

  public List<MutablePair<String, DataType>> getSchema() {
    return this.schema;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return this.name;
  }

  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  public int getNumRows() {
    return this.numRows;
  }

  public int getNumColumns() {
    return this.schema.size();

   /**
   *
   * @param type
   * @return name of random column of given type
   */
  public String getRandomColumn(String columnName, DataType type) {
    List<MutablePair<String, DataType>> columns = new ArrayList<>();
    for (MutablePair<String, DataType> col: this.schema) {
      if (col.getRight() == type) columns.add(col);
    }
    int newColumnProbability = Utils.getRandomInteger(columns.size());
    // add new column of specified datatype with probability 1/(n+1)
    // where n is the number of existing columns of that datatype
    if (newColumnProbability == 0) {
      addColumn(columnName, type);
      return columnName;
    }
    MutablePair<String, DataType> p = Utils.getRandomElement(columns);
    return p.getLeft();
  }

  /**
   *
   * @param type
   * @return name of random column of given type
   */
  public String getRandomColumn(String columnName, DataType type) {
    List<MutablePair<String, DataType>> columns = new ArrayList<>();
    for (MutablePair<String, DataType> col: this.schema) {
      if (col.getRight() == type) columns.add(col);
    }
    int newColumnProbability = Utils.getRandomInteger(columns.size());
    // add new column of specified datatype with probability 1/(n+1)
    // where n is the number of existing columns of that datatype
    if (newColumnProbability == 0) {
      addColumn(columnName, type);
      return columnName;
    }
    MutablePair<String, DataType> p = Utils.getRandomElement(columns);
    return p.getLeft();
  }

  /**
   *
   * @param numRows number of rows of data to generate
   * @param dataType type of data to generate
   * @return column of data with type dataType and numRows rows
   * @throws IllegalArgumentException
   */
  public List<?> generateColumn(int numRows, DataType dataType) throws IllegalArgumentException {
    if (dataType.isIntegerType()) {
      List<Integer> data = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomIntegerData(dataType));
      }
      return data;
    } else if (dataType.isLongType()) {
      List<Long> data = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomLongData(dataType));
      }
      return data;
    } else if (dataType.isDoubleType()) {
      List<Double> data = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomDoubleData(dataType));
      }
      return data;
    } else if (dataType.isBigDecimalType()) {
      List<BigDecimal> data = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomBigDecimalData(dataType));
      }
      return data;
    } else if (dataType.isStringType()) {
      List<String> data = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomStringData(dataType));
      }
      return data;
    } else if (dataType.isBooleanType()) {
      List<Boolean> data = new ArrayList<>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomBooleanData(dataType));
      }
      return data;
    } else {
      throw new IllegalArgumentException("invalid datatype");
    }
  }

  /**
   *
   * @return sample data with number of rows being number of rows in table
   */
  public List<List<?>> generateData() {
    return generateData(this.numRows);
  }

  /**
   *
   * @param numRows number of rows to generate
   * @return sample data with number of rows being numRows
   */
  public List<List<?>> generateData(int numRows) {
    List<List<?>> data = new ArrayList<>();
    for (int i = 0; i < this.schema.size(); i++) {
      List<?> column = this.generateColumn(numRows, this.schema.get(i).getRight());
      data.add(column);
    }
    return data;
  }
}
