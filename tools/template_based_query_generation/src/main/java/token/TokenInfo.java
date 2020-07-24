package token;

/**
 * class representing token information to be parsed from json mapping files
 */
public class TokenInfo {
  private TokenType tokenType;
  private boolean required;
  private int count;

  public TokenType getTokenType() {
    return this.tokenType;
  }

  public void setTokenType(TokenType tokenType) {
    this.tokenType = tokenType;
  }

  public boolean getRequired() {
    return this.required;
  }

  public void setRequired(boolean required) {
    this.required = required;
  }

  public int getCount() {
    return this.count;
  }

  public void setCount(int count) {
    this.count = count;
  }
}