package token;

/**
 * Helper class that indicates what tokens need to follow the associated keyword
 */
public class TokenInfo {

  /* token name */
  private String tokenName;

  /* indicates whether the token is required */
  private boolean required;

  /* indicates how many tokens of this type are needed */
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
