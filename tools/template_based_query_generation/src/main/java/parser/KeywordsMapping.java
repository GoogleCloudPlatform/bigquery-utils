package parser;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Maps features to mappings between dialects and the features' representations in those dialects
 */
public class KeywordsMapping {

  private final String filePathDDL = "./src/main/resources/dialect_config/ddl_mapping.json";
  private final String filePathDML = "./src/main/resources/dialect_config/dml_mapping.json";
  private final String filePathDQL = "./src/main/resources/dialect_config/dql_mapping.json";

  private ImmutableMap<String, Map<String, String>> mapDDL = new ImmutableMap.Builder<String, Map<String, String>>().build();
  private ImmutableMap<String, Map<String, String>> mapDML = new ImmutableMap.Builder<String, Map<String, String>>().build();
  private ImmutableMap<String, Map<String, String>> mapDQL = new ImmutableMap.Builder<String, Map<String, String>>().build();

  /**
   * Constructor of keywords mapping, parsed from the config file
   */
  public KeywordsMapping() {
    try {
      mapDDL = Utils.makeImmutableKeywordMap(Paths.get(filePathDDL));
      mapDML = Utils.makeImmutableKeywordMap(Paths.get(filePathDML));
      mapDQL = Utils.makeImmutableKeywordMap(Paths.get(filePathDQL));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Fetches the appropriate DDL, DML, or DQL keyword mapping
   *
   * @param rawKeyword the keyword to categorize
   * @return the dialect mappings associated with the keyword
   */
  public Map<String, String> getLanguageMap(String rawKeyword) {
    if (mapDDL.containsKey(rawKeyword)) {
      return getMappingDDL(rawKeyword);
    } else if (mapDML.containsKey(rawKeyword)) {
      return getMappingDML(rawKeyword);
    } else {
      return getMappingDQL(rawKeyword);
    }
  }

  /**
   * Returns the dialect map associated with a DDL feature
   *
   * @param word the DDL word to be translated
   * @return the dialect map associated with the word
   * @throws IllegalArgumentException if the DDL set does not contain the word
   */
  private Map<String, String> getMappingDDL(String word) throws IllegalArgumentException {
    if (!mapDDL.containsKey(word)) {
      throw new IllegalArgumentException("The word is not in the DDL set");
    }

    return mapDDL.get(word);
  }

  /**
   * Returns the dialect map associated with a DML feature
   *
   * @param word the DML word to be translated
   * @return the dialect map associated with the word
   * @throws IllegalArgumentException if the DML set does not contain the word
   */
  private Map<String, String> getMappingDML(String word) throws IllegalArgumentException {
    if (!mapDML.containsKey(word)) {
      throw new IllegalArgumentException("The word is not in the DML set");
    }

    return mapDML.get(word);
  }

  /**
   * Returns the dialect map associated with a DQL feature
   *
   * @param word the DQL word to be translated
   * @return the dialect map associated with the word
   * @throws IllegalArgumentException if the DQL set does not contain the word
   */
  private Map<String, String> getMappingDQL(String word) throws IllegalArgumentException {
    if (!mapDQL.containsKey(word)) {
      throw new IllegalArgumentException("The word is not in the DQL set");
    }

    return mapDQL.get(word);
  }
}
