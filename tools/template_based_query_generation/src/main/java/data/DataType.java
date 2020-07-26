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
}

