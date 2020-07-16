package token;

/**
 * class representing token information to be parsed from json mapping files
 */
public class TokenInfo {
  private String tokenName;
  private boolean required;
  private int count;

  public String getTokenName() {
    return this.tokenName;
  }

  public void setTokenName(String tokenName) {
    this.tokenName = tokenName;
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