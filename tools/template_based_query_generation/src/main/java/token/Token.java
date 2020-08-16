package token;

/**
 * class that represents a token or expression
 */
public class Token {
  private String tokenPlaceHolder;
  private String postgresTokenExpression;
  private String bigQueryTokenExpression;
  private TokenInfo tokenInfo;

  public Token(TokenInfo tokenInfo) {
    this.tokenInfo = tokenInfo;
  }

  public Token(String tokenPlaceHolder, String postgresTokenExpression, String bigQueryTokenExpression, TokenInfo tokenInfo) {
    this.tokenPlaceHolder = tokenPlaceHolder;
    this.postgresTokenExpression = postgresTokenExpression;
    this.bigQueryTokenExpression = bigQueryTokenExpression;
    this.tokenInfo = tokenInfo;
  }

  public void setTokenPlaceHolder(String tokenPlaceHolder) {
    this.tokenPlaceHolder = tokenPlaceHolder;
  }

  public String getTokenPlaceHolder() {
    return tokenPlaceHolder;
  }

  public void setPostgresTokenExpression(String postgresTokenExpression) {
    this.postgresTokenExpression = postgresTokenExpression;
  }

  public String getPostgresTokenExpression() {
    return postgresTokenExpression;
  }

  public void setBigQueryTokenExpression(String bigQueryTokenExpression) {
    this.bigQueryTokenExpression = bigQueryTokenExpression;
  }

  public String getBigQueryTokenExpression() {
    return bigQueryTokenExpression;
  }

  public void setTokenInfo(TokenInfo tokenInfo) {
    this.tokenInfo = tokenInfo;
  }

  public TokenInfo getTokenInfo() {
    return tokenInfo;
  }
}
