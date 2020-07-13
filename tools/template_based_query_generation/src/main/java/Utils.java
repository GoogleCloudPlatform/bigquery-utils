import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utilities class that provides random and IO helper functions.
 */
public class Utils {

  private static final ThreadLocalRandom random = ThreadLocalRandom.current();

  private static final int lowerBound = 1;

  private static final String CHARSET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

  /**
   * Helper class that returns all KeywordIndicator(s) for JSON deserialization
   */
  private class Config {
    private List<KeywordIndicator> keywordIndicators;

    public List<KeywordIndicator> getKeywordIndicators() {
      return this.keywordIndicators;
    }

    public void setKeywords(List<KeywordIndicator> keywordIndicators) {
      this.keywordIndicators = keywordIndicators;
    }
  }

  /**
   * Helper class that indicates whether a keyword is included by the user via the user-defined config file
   */
  private class KeywordIndicator {
    private String keyword;
    private boolean isIncluded;

    public String getKeyword() {
      return this.keyword;
    }

    public void setKeyword(String keyword) {
      this.keyword = keyword;
    }

    public boolean getIsIncluded() {
      return this.isIncluded;
    }

    public void setIsIncluded(boolean isIncluded) {
      this.isIncluded = isIncluded;
    }
  }

  /**
   * Returns a random integer between a lowerBound and an upperBound, inclusive
   *
   * @param upperBound a non-negative integer upper bound on the generated random integer, inclusive
   * @return a random integer between lowerBound and upperBound, inclusive
   * @throws IllegalArgumentException if upperBound is negative
   */
  public static int getRandomInteger(int upperBound) throws IllegalArgumentException {
    if (upperBound < 1) {
      throw new IllegalArgumentException("Upper bound cannot be nonpositive");
    }

    return random.nextInt(lowerBound, upperBound + 1);
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
   * Writes generated outputs to a specified directory, creating one if it doesn't exist.
   *
   * @param outputs         collection of statements to write
   * @param outputDirectory relative path of a specified directory
   * @throws IOException if the IO fails or creating the necessary files or folders fails
   */
  public static void writeDirectory(ImmutableMap<String, ImmutableList<String>> outputs, Path outputDirectory) throws IOException {
    writeFile(outputs.get("BQ_skeletons"), outputDirectory.resolve("bq_skeleton.txt"));
    writeFile(outputs.get("BQ_tokenized"), outputDirectory.resolve("bq_tokenized.txt"));
    writeFile(outputs.get("Postgre_skeletons"), outputDirectory.resolve("postgre_skeleton.txt"));
    writeFile(outputs.get("Postgre_tokenized"), outputDirectory.resolve("postgre_tokenized.txt"));
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
  public static ImmutableSet<String> makeImmutableSet(Path inputPath) throws IOException {
    BufferedReader reader = Files.newBufferedReader(inputPath, UTF_8);
    Gson gson = new Gson();
    Config config = gson.fromJson(reader, Config.class);

    ImmutableList.Builder<String> builder = ImmutableList.builder();

    for (KeywordIndicator keywordIndicator : config.getKeywordIndicators()) {
      if (keywordIndicator.getIsIncluded()) {
        builder.add(keywordIndicator.getKeyword());
      }
    }

    ImmutableList<String> list = builder.build();

    return ImmutableSet.copyOf(list);
  }

  /**
   * Creates an immutable map from the user-defined config file of keyword mappings
   *
   * @param fileName relative path of the config file
   * @return an immutable map between user-defined keywords and PostgreSQL or BigQuery from the config file
   */
  public static ImmutableMap<String, String> makeImmutableMap(String fileName, ImmutableSet<String> keywords) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileName), UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!(line.charAt(0) == '/' && line.charAt(1) == '/')) {
          String[] pair = line.split(":");
          if (keywords.contains(pair[0])) {
            builder.put(pair[0], pair[1]);
          }
        }
      }
    } catch (IOException exception) {
      System.out.println(exception);
    }

    ImmutableMap<String, String> map = builder.build();

    return map;
  }

  // TODO(spoiledhua): refactor IO exception handling
}
