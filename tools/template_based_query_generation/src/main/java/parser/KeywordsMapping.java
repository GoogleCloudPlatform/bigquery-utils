package parser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.nio.file.Paths;

public class KeywordsMapping {

  private final String filePathDDL = "./src/main/resources/dialect_config/ddl_mapping.json";
  private final String filePathDML = "./src/main/resources/dialect_config/dml_mapping.json";
  private final String filePathDQL = "./src/main/resources/dialect_config/dql_mapping.json";

  private ImmutableMap<String, ImmutableList<Mapping>> mapDDL = new ImmutableMap.Builder<String, ImmutableList<Mapping>>().build();
  private ImmutableMap<String, ImmutableList<Mapping>> mapDML = new ImmutableMap.Builder<String, ImmutableList<Mapping>>().build();
  private ImmutableMap<String, ImmutableList<Mapping>> mapDQL = new ImmutableMap.Builder<String, ImmutableList<Mapping>>().build();

  private final Keywords keywords = new Keywords();

  /**
   * Constructor of keywords mapping, parsed from the config file
   */
  public KeywordsMapping() {
    try {
      mapDDL = Utils.makeImmutableKeywordMap(Paths.get(filePathDDL), keywords.getKeywordsDDL());
      mapDML = Utils.makeImmutableKeywordMap(Paths.get(filePathDML), keywords.getKeywordsDML());
      mapDQL = Utils.makeImmutableKeywordMap(Paths.get(filePathDQL), keywords.getKeywordsDQL());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Returns the list of possible PostgreSQL, BigQuery, and Token mappings to a DDL word
   *
   * @param word the DDL word to be translated
   * @return the list of possible PostgreSQL, BigQuery, and Token mappings to the word
   * @throws IllegalArgumentException if the DDL set does not contain the word
   */
  public ImmutableList<Mapping> getMappingDDL(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDDL(word)) {
      throw new IllegalArgumentException("The word is not in the DDL set");
    }

    return mapDDL.get(word);
  }

  /**
   * Returns the list of possible PostgreSQL, BigQuery, and Token mappings to a DML word
   *
   * @param word the DML word to be translated
   * @return the list of possible PostgreSQL, BigQuery, and Token mappings to the word
   * @throws IllegalArgumentException if the DML set does not contain the word
   */
  public ImmutableList<Mapping> getMappingDML(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDML(word)) {
      throw new IllegalArgumentException("The word is not in the DML set");
    }

    return mapDML.get(word);
  }

  /**
   * Returns the list of possible PostgreSQL, BigQuery, and Token mappings to a DQL word
   *
   * @param word the DQL word to be translated
   * @return the list of possible PostgreSQL, BigQuery, and Token mappings to the word
   * @throws IllegalArgumentException if the DQL set does not contain the word
   */
  public ImmutableList<Mapping> getMappingDQL(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDQL(word)) {
      throw new IllegalArgumentException("The word is not in the DQL set");
    }

    return mapDQL.get(word);
  }
}
