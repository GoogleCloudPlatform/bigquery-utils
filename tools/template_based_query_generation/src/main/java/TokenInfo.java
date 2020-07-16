/**
 * Helper class that indicates what tokens need to follow the associated keyword
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
