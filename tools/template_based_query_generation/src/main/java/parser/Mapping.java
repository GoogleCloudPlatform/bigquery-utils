package parser;

import token.TokenInfo;

import java.util.List;

/**
 * Helper class that lists PostgreSQL and BigQuery mappings and necessary tokens for all keyword variants
 */
public class Mapping {
  private String postgre;
  private String bigQuery;
  private List<TokenInfo> tokenInfos;

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

  public List<TokenInfo> getTokenInfos() {
    return this.tokenInfos;
  }

  public void setTokenInfos(List<TokenInfo> tokenInfos) {
    this.tokenInfos = tokenInfos;
  }
}
