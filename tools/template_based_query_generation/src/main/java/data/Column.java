package data;

import parser.Utils;

import java.math.BigDecimal;
import java.util.ArrayList;

public class Column {

  DataType dataType;
  private ArrayList<Integer> integerList;
  private ArrayList<Long> longList;
  private ArrayList<Double> doubleList;
  private ArrayList<BigDecimal> bigDecimalList;
  private ArrayList<String> stringList;
  private ArrayList<Boolean> booleanList;

  public Column(DataType dataType) {
    this.dataType = dataType;
  }

  public ArrayList<? extends Object> getData() {
    if (this.dataType.isIntegerType()) {
      return this.integerList;
    } else if (this.dataType.isLongType()) {
      return this.longList;
    } else if (this.dataType.isDoubleType()) {
      return this.doubleList;
    } else if (this.dataType.isBigDecimalType()) {
      return this.bigDecimalList;
    } else if (this.dataType.isStringType()) {
      return this.stringList;
    } else {
      return this.booleanList;
    }
  }

  public void setDataInteger(ArrayList<Integer> data) throws IllegalArgumentException {
    if (!this.dataType.isIntegerType()) {
      throw new IllegalArgumentException("column datatype cannot support data in format of ArrayList<Integer>");
    }
    this.integerList = data;
  }

  public void setDataLong(ArrayList<Long> data) throws IllegalArgumentException {
    if (!this.dataType.isLongType()) {
      throw new IllegalArgumentException("column datatype cannot support data in format of ArrayList<Long>");
    }
    this.longList = data;
  }

  public void setDataDouble(ArrayList<Double> data) throws IllegalArgumentException {
    if (!this.dataType.isDoubleType()) {
      throw new IllegalArgumentException("column datatype cannot support data in format of ArrayList<Double>");
    }
    this.doubleList = data;
  }

  public void setDataBigDecimal(ArrayList<BigDecimal> data) throws IllegalArgumentException {
    if (!this.dataType.isBigDecimalType()) {
      throw new IllegalArgumentException("column datatype cannot support data in format of ArrayList<BigDecimal>");
    }
    this.bigDecimalList = data;
  }

  public void setDataString(ArrayList<String> data) throws IllegalArgumentException {
    if (!this.dataType.isStringType()) {
      throw new IllegalArgumentException("column datatype cannot support data in format of ArrayList<String>");
    }
    this.stringList = data;
  }

  public void setDataBoolean(ArrayList<Boolean> data) throws IllegalArgumentException {
    if (!this.dataType.isBooleanType()) {
      throw new IllegalArgumentException("column datatype cannot support data in format of ArrayList<Boolean>");
    }
    this.booleanList = data;
  }

  public void fillRandomData(int numRows) {
    if (this.dataType.isIntegerType()) {
      ArrayList<Integer> data = new ArrayList<Integer>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomIntegerData(this.dataType));
      }
      this.integerList = data;
    } else if (this.dataType.isLongType()) {
      ArrayList<Long> data = new ArrayList<Long>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomLongData(this.dataType));
      }
      this.longList = data;
    } else if (this.dataType.isDoubleType()) {
      ArrayList<Double> data = new ArrayList<Double>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomDoubleData(this.dataType));
      }
      this.doubleList = data;
    } else if (this.dataType.isBigDecimalType()) {
      ArrayList<BigDecimal> data = new ArrayList<BigDecimal>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomBigDecimalData(this.dataType));
      }
      this.bigDecimalList = data;
    } else if (this.dataType.isStringType()) {
      ArrayList<String> data = new ArrayList<String>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomStringData(this.dataType));
      }
      this.stringList = data;
    } else if (this.dataType.isBooleanType()) {
      ArrayList<Boolean> data = new ArrayList<Boolean>();
      for (int i = 0; i < numRows; i++) {
        data.add(Utils.generateRandomBooleanData(this.dataType));
      }
      this.booleanList = data;
    }
  }



}
