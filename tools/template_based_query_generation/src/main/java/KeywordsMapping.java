import com.google.common.collect.ImmutableMap;

public class KeywordsMapping {

  private final String filePathPostgreDDL = "./src/main/resources/dialect_config/ddl_mapping_postgre.json";
  private final String filePathPostgreDML = "./src/main/resources/dialect_config/dml_mapping_postgre.json";
  private final String filePathPostgreDQL = "./src/main/resources/dialect_config/dql_mapping_postgre.json";
  private final String filePathBigQueryDDL = "./src/main/resources/dialect_config/ddl_mapping_BQ.json";
  private final String filePathBigQueryDML = "./src/main/resources/dialect_config/dml_mapping_BQ.json";
  private final String filePathBigQueryDQL = "./src/main/resources/dialect_config/dql_mapping_BQ.json";

  private final ImmutableMap<String, String> mapPostgreDDL;
  private final ImmutableMap<String, String> mapPostgreDML;
  private final ImmutableMap<String, String> mapPostgreDQL;
  private final ImmutableMap<String, String> mapBigQueryDDL;
  private final ImmutableMap<String, String> mapBigQueryDML;
  private final ImmutableMap<String, String> mapBigQueryDQL;

  private final Keywords keywords = new Keywords();

  /**
   * Constructor of keywords mapping, parsed from the config file
   */
  public KeywordsMapping() {
    mapPostgreDDL = Utils.makeImmutableMap(filePathPostgreDDL, keywords.getKeywordsDDL());
    mapPostgreDML = Utils.makeImmutableMap(filePathPostgreDML, keywords.getKeywordsDML());
    mapPostgreDQL = Utils.makeImmutableMap(filePathPostgreDQL, keywords.getKeywordsDQL());
    mapBigQueryDDL = Utils.makeImmutableMap(filePathBigQueryDDL, keywords.getKeywordsDDL());
    mapBigQueryDML = Utils.makeImmutableMap(filePathBigQueryDML, keywords.getKeywordsDML());
    mapBigQueryDQL = Utils.makeImmutableMap(filePathBigQueryDQL, keywords.getKeywordsDQL());
  }

  /**
   * Returns the PostgreSQL mapping to a DDL word
   *
   * @param word the DDL word to be translated
   * @return the PostgreSQL mapping to the word
   * @throws IllegalArgumentException if the DDL set does not contain the word
   */
  public String getMappingPostgreDDL(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDDL(word)) {
      throw new IllegalArgumentException("The word is not in the DDL set");
    }

    return mapPostgreDDL.get(word);
  }

  /**
   * Returns the PostgreSQL mapping to a DML word
   *
   * @param word the DML word to be translated
   * @return the PostgreSQL mapping to the word
   * @throws IllegalArgumentException if the DML set does not contain the word
   */
  public String getMappingPostgreDML(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDML(word)) {
      throw new IllegalArgumentException("The word is not in the DML set");
    }

    return mapPostgreDML.get(word);
  }

  /**
   * Returns the PostgreSQL mapping to a DQL word
   *
   * @param word the DQL word to be translated
   * @return the PostgreSQL mapping to the word
   * @throws IllegalArgumentException if the DQL set does not contain the word
   */
  public String getMappingPostgreDQL(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDQL(word)) {
      throw new IllegalArgumentException("The word is not in the DQL set");
    }

    return mapPostgreDQL.get(word);
  }

  /**
   * Returns the BigQuery mapping to a DDL word
   *
   * @param word the DDL word to be translated
   * @return the BigQuery mapping to the keyword
   * @throws IllegalArgumentException if the DDL set does not contain the word
   */
  public String getMappingBigQueryDDL(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDDL(word)) {
      throw new IllegalArgumentException("The word is not in the DDL set");
    }

    return mapBigQueryDDL.get(word);
  }

  /**
   * Returns the BigQuery mapping to a DML word
   *
   * @param word the DML word to be translated
   * @return the BigQuery mapping to the keyword
   * @throws IllegalArgumentException if the DML set does not contain the word
   */
  public String getMappingBigQueryDML(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDML(word)) {
      throw new IllegalArgumentException("The word is not in the DML set");
    }

    return mapBigQueryDML.get(word);
  }

  /**
   * Returns the BigQuery mapping to a DQL word
   *
   * @param word the DQL word to be translated
   * @return the BigQuery mapping to the keyword
   * @throws IllegalArgumentException if the DQL set does not contain the word
   */
  public String getMappingBigQueryDQL(String word) throws IllegalArgumentException {
    if (!keywords.inKeywordsDQL(word)) {
      throw new IllegalArgumentException("The word is not in the DQL set");
    }

    return mapBigQueryDQL.get(word);
  }
}
