package parser;

import token.TokenInfo;

import java.util.List;

/**
 * Helper class that lists PostgreSQL and BigQuery mappings and necessary tokens for all keyword variants
 */
public class Mapping {

  /* Equivalent PostgreSQL mapping to a keyword */
  private String postgres;

  /* Equivalent BigQuery mapping to a keyword */
  private String bigQuery;

  /* All necessary tokens for a given keyword variant */
  private List<TokenInfo> tokenInfos;

  public String getPostgres() {
    return this.postgres;
  }

  public void setPostgres(String postgres) {
    this.postgres = postgres;
  }

  public String getBigQuery() {
    return this.bigQuery;
  }

  public void setBigQuery(String bigQuery) {
    this.bigQuery = bigQuery;
  }

  public List<TokenInfo> getTokenInfos() {
    return this.tokenInfos;
  }

  public void setTokenInfos(List<TokenInfo> tokenInfos) {
    this.tokenInfos = tokenInfos;
  }
}
