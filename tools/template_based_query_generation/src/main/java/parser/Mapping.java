package parser;

import token.TokenInfo;

import java.util.List;

/**
 * Helper class that lists PostgreSQL and BigQuery mappings and necessary tokens for all keyword variants
 */
public class Mapping {

  /* List of dialect maps to each keyword */
  private List<DialectMap> dialectMaps;

  /* All necessary tokens for a given keyword variant */
  private List<TokenInfo> tokenInfos;

  public List<DialectMap> getDialectMaps() {
    return this.dialectMaps;
  }

  public void setDialectMaps(List<DialectMap> dialectMaps) {
    this.dialectMaps = dialectMaps;
  }

  public List<TokenInfo> getTokenInfos() {
    return this.tokenInfos;
  }

  public void setTokenInfos(List<TokenInfo> tokenInfos) {
    this.tokenInfos = tokenInfos;
  }
}
