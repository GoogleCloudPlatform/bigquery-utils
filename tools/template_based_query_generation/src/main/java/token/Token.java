package token;

import java.util.Map;

/**
 * class that represents a token or expression
 */
public class Token {
  private String tokenPlaceHolder;
  private Map<String, String> dialectExpressions;
  private TokenInfo tokenInfo;

  public Token(TokenInfo tokenInfo) {
    this.tokenInfo = tokenInfo;
  }

  public Token(String tokenPlaceHolder, Map<String, String> dialectExpressions, TokenInfo tokenInfo) {
    this.tokenPlaceHolder = tokenPlaceHolder;
    this.dialectExpressions = dialectExpressions;
    this.tokenInfo = tokenInfo;
  }

  public void setTokenPlaceHolder(String tokenPlaceHolder) {
    this.tokenPlaceHolder = tokenPlaceHolder;
  }

  public String getTokenPlaceHolder() {
    return tokenPlaceHolder;
  }

  public void setDialectExpressions(Map<String, String> dialectExpressions) {
    this.dialectExpressions = dialectExpressions;
  }

  public Map<String, String> getDialectExpressions() {
    return dialectExpressions;
  }

  public void setTokenInfo(TokenInfo tokenInfo) {
    this.tokenInfo = tokenInfo;
  }

  public TokenInfo getTokenInfo() {
    return tokenInfo;
  }
}
