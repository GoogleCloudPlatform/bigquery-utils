package parser;

import token.TokenInfo;

import java.util.List;
import java.util.Map;

/**
 * Helper class that lists PostgreSQL and BigQuery mappings and necessary tokens for all keyword variants
 */
public class Mapping {

  /* List of dialect maps to each keyword */
  private Map<String, String> dialectMap;

  /* All necessary tokens for a given keyword variant */
  private List<TokenInfo> tokenInfos;

  public Map<String, String> getDialectMap() {
    return this.dialectMap;
  }

  public void setDialectMap(Map<String, String> dialectMap) {
    this.dialectMap = dialectMap;
  }

  public List<TokenInfo> getTokenInfos() {
    return this.tokenInfos;
  }

  public void setTokenInfos(List<TokenInfo> tokenInfos) {
    this.tokenInfos = tokenInfos;
  }
}
