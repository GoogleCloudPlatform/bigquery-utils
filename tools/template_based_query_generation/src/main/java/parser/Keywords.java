package parser;

import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Set of user-specified DDL, DML, and DQL keywords
 */
public class Keywords {

  private final String filePathDDL = "./src/main/resources/user_config/ddl.json";
  private final String filePathDML = "./src/main/resources/user_config/dml.json";
  private final String filePathDQL = "./src/main/resources/user_config/dql.json";

  private ImmutableSet<String> setDDL = new ImmutableSet.Builder<String>().build();
  private ImmutableSet<String> setDML = new ImmutableSet.Builder<String>().build();
  private ImmutableSet<String> setDQL = new ImmutableSet.Builder<String>().build();

  /**
   * Constructor of user-defined keywords, parsed from the config file
   */
  public Keywords() {
    try {
      setDDL = Utils.makeImmutableKeywordSet(Paths.get(filePathDDL));
      setDML = Utils.makeImmutableKeywordSet(Paths.get(filePathDML));
      setDQL = Utils.makeImmutableKeywordSet(Paths.get(filePathDQL));
    } catch (IOException exception) {
      exception.printStackTrace();
    }
  }

  /**
   * Returns the set of SQL DDL keywords
   *
   * @return the set of SQL DDL keywords
   */
  public ImmutableSet<String> getKeywordsDDL() {
    return setDDL;
  }

  /**
   * Returns the set of SQL DML keywords
   *
   * @return the set of SQL DML keywords
   */
  public ImmutableSet<String> getKeywordsDML() {
    return setDML;
  }

  /**
   * Returns the set of SQL DQL keywords
   *
   * @return the set of SQL DQL keywords
   */
  public ImmutableSet<String> getKeywordsDQL() {
    return setDQL;
  }

  /**
   * Checks whether a word is in the DDL set of user-specified keywords
   *
   * @param word the word to check
   * @return a boolean indicating if the word is in the DDL keywords set
   */
  public boolean inKeywordsDDL(String word) {
    return setDDL.contains(word);
  }

  /**
   * Checks whether a word is in the DML set of user-specified keywords
   *
   * @param word the word to check
   * @return a boolean indicating if the word is in the DML keywords set
   */
  public boolean inKeywordsDML(String word) {
    return setDML.contains(word);
  }

  /**
   * Checks whether a word is in the DQL set of user-specified keywords
   *
   * @param word the word to check
   * @return a boolean indicating if the word is in the DQL keywords set
   */
  public boolean inKeywordsDQL(String word) {
    return setDQL.contains(word);
  }
}
