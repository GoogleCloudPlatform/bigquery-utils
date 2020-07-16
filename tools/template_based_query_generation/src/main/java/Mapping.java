import java.util.List;

/**
 * Helper class that lists PostgreSQL and BigQuery mappings and necessary tokens for all keyword variants
 */
public class Mapping {

  /* Equivalent PostgreSQL mapping to a keyword */
  private String postgre;

  /* Equivalent BigQuery mapping to a keyword */
  private String bigQuery;

  /* All necessary tokens for a given keyword variant */
  private List<TokenInfo> tokens;

  public String getPostgre() {
    return this.postgre;
  }

  public void setPostgre(String postgre) {
    this.postgre = postgre;
  }

  public String getBigQuery() {
    return this.bigQuery;
  }

  public void setBigQuery(String bigQuery) {
    this.bigQuery = bigQuery;
  }

  public List<TokenInfo> getTokens() {
    return this.tokens;
  }

  public void setTokens(List<TokenInfo> tokens) {
    this.tokens = tokens;
  }
}
