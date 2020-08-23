package token;

import java.util.Map;

/**
 * class that represents a token or expression
 */
public class Token {
  private Map<String, String> dialectExpressions;
  private TokenInfo tokenInfo;

  public Token(TokenInfo tokenInfo) {
    this.tokenInfo = tokenInfo;
  }

  public Token(Map<String, String> dialectExpressions, TokenInfo tokenInfo) {
    this.dialectExpressions = dialectExpressions;
    this.tokenInfo = tokenInfo;
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
