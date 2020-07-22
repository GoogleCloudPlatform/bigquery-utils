package token;

/**
 * class that represents a token or expression
 */
public class Token {
  private String tokenPlaceHolder;
  private String tokenExpression;
  private TokenInfo tokenInfo;

  public Token(TokenInfo tokenInfo) {
    this.tokenInfo = tokenInfo;
  }

  public Token(String tokenPlaceHolder, String tokenExpression, TokenInfo tokenInfo) {
    this.tokenPlaceHolder = tokenPlaceHolder;
    this.tokenExpression = tokenExpression;
    this.tokenInfo = tokenInfo;
  }

  public void setTokenPlaceHolder(String tokenPlaceHolder) {
    this.tokenPlaceHolder = tokenPlaceHolder;
  }

  public String getTokenPlaceHolder() {
    return tokenPlaceHolder;
  }

  public void setTokenExpression(String tokenExpression) {
    this.tokenExpression = tokenExpression;
  }

  public String getTokenExpression() {
    return tokenExpression;
  }

  public void setTokenInfo(TokenInfo tokenInfo) {
    this.tokenInfo = tokenInfo;
  }

  public TokenInfo getTokenInfo() {
    return tokenInfo;
  }
}
