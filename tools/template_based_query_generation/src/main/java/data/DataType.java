package data;

import java.util.Random;

/**
 * Data types in sql dialects
 */
public enum DataType {
  SMALL_INT,
  INTEGER,
  BIG_INT,
  DECIMAL,
  NUMERIC,
  REAL,
  BIG_REAL,
  SMALL_SERIAL,
  SERIAL,
  BIG_SERIAL,
  BOOL,
  STR,
  BYTES,
  DATE,
  TIME,
  TIMESTAMP;

  /**
   * Pick a random value of the DataType enum.
   * @return a random BaseColor.
   */
  public static DataType getRandomDataType() {
    Random random = new Random();
    return values()[random.nextInt(values().length)];
  }

  /**
   *
   * @return true if can be represented by an integer
   */
  public boolean isIntegerType() {
    return this == SMALL_INT || this == INTEGER || this == SMALL_SERIAL || this == SERIAL;
  }

  /**
   *
   * @return true if can be represented by a long
   */
  public boolean isLongType() {
    return this == BIG_INT || this == BIG_SERIAL;
  }

  /**
   *
   * @return true if can be represented by an double
   */
  public boolean isDoubleType() {
    return this == BIG_REAL || this == REAL;
  }

  /**
   *
   * @return true if can be represented by a big decimal
   */
  public boolean isBigDecimalType() {
    return this == NUMERIC || this == DECIMAL;
  }

  /**
   *
   * @return true if can be represented by a String
   */
  public boolean isStringType() {
    return this == STR || this == BYTES || this == DATE || this == TIME || this == TIMESTAMP;
  }

  /**
   *
   * @return true if can be represented by a boolean
   */
  public boolean isBooleanType() {
    return this == BOOL;
  }

}

