package parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import data.DataType;
import org.apache.commons.lang3.tuple.MutablePair;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utilities class that provides random and IO helper functions.
 */
public class Utils {

  private static final ThreadLocalRandom random = ThreadLocalRandom.current();

  private static final int lowerBound = 0;

  private static final String CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

  /**
   * Returns a random integer between a lowerBound and an upperBound, inclusive
   *
   * @param upperBound a non-negative integer upper bound on the generated random integer, inclusive
   * @return a random integer between lowerBound and upperBound, inclusive
   * @throws IllegalArgumentException if upperBound is negative
   */
  public static int getRandomInteger(int upperBound) throws IllegalArgumentException {
    if (upperBound < 0) {
      throw new IllegalArgumentException("Upper bound cannot be negative");
    }

    return random.nextInt(lowerBound, upperBound + 1);
  }

  /**
   * Returns a random element from given set
   * @param list a list of objects from which a random element is selected
   */
  public static MutablePair<String, DataType> getRandomElement(ArrayList<MutablePair<String, DataType>> list) throws IllegalArgumentException  {
    if (list.size() <= 0) {
      throw new IllegalArgumentException("ArrayList must contain at least one element");
    }
    int index = Utils.getRandomInteger(list.size()-1);
    return list.get(index);
  }

  /**
   * Returns a random string with a specified length that matches the regex '[a-zA-Z_]'
   *
   * @param length a nonzero integer specifying the desired length of the generated string
   * @return a random string that matches the regex '[a-zA-Z_]' and has the specified length
   */
  public static String getRandomString(int length) throws IllegalArgumentException {
    if (length <= 0) {
      throw new IllegalArgumentException("Random string must have positive length");
    }

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < length; i++) {
      char randomChar = CHARSET.charAt(random.nextInt(0, CHARSET.length()));
      if (i == 0 && Character.isDigit(randomChar)) {
        // SQL identifiers can't start with digits, so replace with an arbitrary character
        randomChar = CHARSET.charAt(random.nextInt(0, 52));
      }
      sb.append(randomChar);
    }

    return sb.toString();
  }

  /**
   * Returns a random string with a specified length consisting of 0s and 1s
   *
   * @param length a nonzero integer specifying the desired length of the generated string
   * @return a random string that matches the regex '[0|1]*' and has the specified length
   */
  public static String getRandomStringBytes(int length) throws IllegalArgumentException {
    if (length <= 0) {
      throw new IllegalArgumentException("Random byte string must have positive length");
    }

    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < length; i++) {
      if (random.nextBoolean()) {
        sb.append("1");
      } else{
        sb.append("0");
      }
    }

    return sb.toString();
  }

  /**
   *
   * @return a random string representing a random date between 0001-01-01 and 9999-12-31 formatted as YYYY-MM-dd
   */
  public static String getRandomStringDate() {
    Date d1 = new Date(-2177434800000L);
    Date d2 = new Date(253402232400000L);
    Date randomDate = new Date(random.nextLong(d1.getTime(), d2.getTime()));
    SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
    String date = dateFormat.format(randomDate);
    return date;
  }

  /**
   *
   * @return a random string representing a random time from 00:00:00 to 23:59:59.99999
   */
  private static String getRandomStringTime() {
    int hour = random.nextInt(24);
    int min = random.nextInt(60);
    int second = random.nextInt(60);
    int milli = random.nextInt(100000);
    return hour + ":" + min + ":" + second + "." + milli;
  }

  /**
   *
   * @return a random string representing a random time from 0001-01-01 00:00:00 to 9999-12-31 23:59:59.99999
   */
  private static String getRandomStringTimestamp() {
    return getRandomStringDate() + " " + getRandomStringTime();
  }

  /**
   * Writes generated outputs to a specified directory, creating one if it doesn't exist.
   *
   * @param outputs         collection of statements to write
   * @param outputDirectory relative path of a specified directory
   * @throws IOException if the IO fails or creating the necessary files or folders fails
   */
  public static void writeDirectory(ImmutableMap<String, ImmutableList<String>> outputs, Path outputDirectory) throws IOException {
    writeFile(outputs.get("PostgreSQL"), outputDirectory.resolve("postgreSQL.txt"));
    writeFile(outputs.get("BigQuery"), outputDirectory.resolve("bigQuery.txt"));
    // TODO(spoiledhua): write sample data to file

    System.out.println("The output is stored at " + outputDirectory);
  }

  /**
   * Writes generated outputs to a default directory, creating one if it doesn't exist.
   *
   * @param outputs collection of statements to write
   * @throws IOException if the IO fails or creating the necessary files or folders fails
   */
  public static void writeDirectory(ImmutableMap<String, ImmutableList<String>> outputs) throws IOException {
    String outputDirectory = getOutputDirectory("outputs");
    File file = new File(outputDirectory);

    if (!file.exists() && !file.mkdir()) {
      throw new FileNotFoundException("The default \"output\" directory could not be created");
    }

    writeDirectory(outputs, file.toPath());
  }

  /**
   * Writes generated statements to a specified file and creates all necessary files.
   *
   * @param statements skeletons or tokenized queries to write
   * @param outputPath absolute path of a specified file
   * @throws IOException if the IO fails or creating the necessary files or folders fails
   */
  public static void writeFile(ImmutableList<String> statements, Path outputPath) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(outputPath, UTF_8)) {
      for (String statement : statements) {
        writer.write(statement);
        writer.write("\n");
      }
    }
  }

  /**
   * Converts the specified directory's relative path to its absolute path.
   *
   * @param directoryName relative path of a specified directory
   * @return absolute path of the specified directory
   */
  private static String getOutputDirectory(String directoryName) {
    final String workingDirectory = System.getProperty("user.dir");
    return workingDirectory + "/" + directoryName;
  }

  /**
   * Creates an immutable set from the user-defined config file of keyword mappings
   *
   * @param inputPath relative path of the config file
   * @return an immutable set of keywords from the config file
   */
  public static ImmutableSet<String> makeImmutableKeywordSet(Path inputPath) throws IOException {

    BufferedReader reader = Files.newBufferedReader(inputPath, UTF_8);
    Gson gson = new Gson();
    FeatureIndicators featureIndicators = gson.fromJson(reader, FeatureIndicators.class);

    ImmutableList.Builder<String> builder = ImmutableList.builder();

    for (FeatureIndicator featureIndicator : featureIndicators.getFeatureIndicators()) {
      if (featureIndicator.getIsIncluded()) {
        builder.add(featureIndicator.getFeature().name());
      }
    }

    ImmutableList<String> list = builder.build();

    return ImmutableSet.copyOf(list);
  }

  /**
   * Creates an immutable map from the config file of keyword mappings
   *
   * @param inputPath relative path of the config file
   * @return an immutable map between user-defined keywords and PostgreSQL or BigQuery from the config file
   */
  public static ImmutableMap<String, ImmutableList<Mapping>> makeImmutableKeywordMap(Path inputPath, ImmutableSet<String> keywords) throws IOException {
    BufferedReader reader = Files.newBufferedReader(inputPath, UTF_8);
    Gson gson = new Gson();
    Features features = gson.fromJson(reader, Features.class);

    ImmutableMap.Builder<String, ImmutableList<Mapping>> builder = ImmutableMap.builder();

    for (Feature feature : features.getFeatures()) {
      if (keywords.contains(feature.getFeature())) {
        builder.put(feature.getFeature(), ImmutableList.copyOf(feature.getAllMappings()));
      }
    }

    ImmutableMap<String, ImmutableList<Mapping>> map = builder.build();

    return map;
  }

  /**
   * Creates an immutable map from the config file of datatype mappings
   *
   * @param inputPath relative path of the config file
   * @return an immutable map between datatypes and PostgreSQL or BigQuery from the config file
   */
  public static ImmutableMap<DataType, Map> makeImmutableDataTypeMap(Path inputPath) throws IOException {
    BufferedReader reader = Files.newBufferedReader(inputPath, UTF_8);
    Gson gson = new Gson();
    DataTypeMaps dataTypeMaps = gson.fromJson(reader, DataTypeMaps.class);

    ImmutableMap.Builder<DataType, Map> builder = ImmutableMap.builder();

    for (DataTypeMap dataTypeMap : dataTypeMaps.getDataTypeMaps()) {
      builder.put(dataTypeMap.getDataType(), dataTypeMap.getDialectMap());
    }

    ImmutableMap<DataType, Map> map = builder.build();

    return map;
  }
  // TODO(spoiledhua): refactor IO exception handling


  /**
   *
   * @param dataType
   * @return random data of type dataType
   * @throws IllegalArgumentException
   */
  public static int generateRandomIntegerData(DataType dataType) throws IllegalArgumentException {
    if (dataType == DataType.SMALL_INT) {
      return 	random.nextInt(-32768,32769);
    } else if (dataType == DataType.INTEGER) {
      return 	random.nextInt();
    } else if (dataType == DataType.SMALL_SERIAL) {
      return 	random.nextInt(1, 32768);
    } else if (dataType == DataType.SERIAL) {
      int num = random.nextInt();
      if (num == Integer.MIN_VALUE) {
        return 0;
      } else {
        return	Math.abs(num);
      }
    } else {
      throw new IllegalArgumentException("dataType cannot be represented by an int type");
    }
  }

  /**
   *
   * @param dataType
   * @return random data of type dataType
   * @throws IllegalArgumentException
   */
  public static long generateRandomLongData(DataType dataType) {
    if (dataType == DataType.BIG_INT) {
      return 	random.nextLong();
    } else if (dataType == DataType.BIG_SERIAL) {
      long num = random.nextLong();
      if (num == Long.MIN_VALUE) {
        return 0;
      } else {
        return	Math.abs(num);
      }
    } else {
      throw new IllegalArgumentException("dataType cannot be represented by a long type");
    }
  }

  /**
   *
   * @param dataType
   * @return random data of type dataType
   * @throws IllegalArgumentException
   */
  public static double generateRandomDoubleData(DataType dataType) {
    if (dataType == DataType.REAL) {
      return random.nextFloat();
    } else if (dataType == DataType.BIG_REAL) {
      return random.nextDouble();
    } else {
      throw new IllegalArgumentException("dataType cannot be represented by a double type");
    }
  }

  /**
   *
   * Up to 131072 digits are permitted in postgres, here uses up to 50 digits (slightly over size of double)
   * @param dataType
   * @return random data of type dataType
   * @throws IllegalArgumentException
   */
  public static BigDecimal generateRandomBigDecimalData(DataType dataType) {
    if (dataType == DataType.DECIMAL) {
      BigDecimal low = new BigDecimal("-500000000000000000000000000000000000000000000000000");
      BigDecimal range = low.abs().multiply(new BigDecimal(2));
      return low.add(range.multiply(new BigDecimal(random.nextDouble(0,1))));
    } else if (dataType == DataType.NUMERIC) {
      BigDecimal low = new BigDecimal("-500000000000000000000000000000000000000000000000000");
      BigDecimal range = low.abs().multiply(new BigDecimal(2));
      return low.add(range.multiply(new BigDecimal(random.nextDouble(0,1))));
    } else {
      throw new IllegalArgumentException("dataType cannot be represented by a big decimal type");
    }
  }

  /**
   *
   * // TODO: factor out constants into config, do date generation, time, and timestamp generation
   * @param dataType
   * @return random data of type dataType
   * @throws IllegalArgumentException
   */
  public static String generateRandomStringData(DataType dataType) {
    if (dataType == DataType.STR) {
      return getRandomString(20);
    } else if (dataType == DataType.BYTES) {
      return getRandomStringBytes(20);
    } else if (dataType == DataType.DATE) {
      return "\'" + getRandomStringDate() + "\'";
    } else if (dataType == DataType.TIME) {
      return "\'" + getRandomStringTime() + "\'";
    } else if (dataType == DataType.TIMESTAMP) {
      return "\'" + getRandomStringTimestamp() + "\'";
    } else {
      throw new IllegalArgumentException("dataType cannot be represented by a string type");
    }
  }

  /**
   *
   * @param dataType
   * @return random data of type dataType
   * @throws IllegalArgumentException
   */
  public static boolean generateRandomBooleanData(DataType dataType) {
    if (dataType == DataType.BOOL) {
      return random.nextBoolean();
    } else {
      throw new IllegalArgumentException("dataType cannot be represented by a boolean type");
    }
  }

}
