import com.sun.tools.javac.parser.Tokens;

import java.util.List;

/**
 * Helper class that lists PostgreSQL and BigQuery mappings and necessary tokens for all keyword variants
 */
public class Mapping {
  private String postgre;
  private String bigQuery;
  private List<Tokens> tokens;

  /**
   * Helper class that indicates what tokens need to follow the associated keyword
   */
  private class Token {
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

  public String getPostgre() {
    return this.postgre;
  }

  public void setPostgre(String postgre) {
    this.postgre = postgre;
  }

  public String getBigQuery() {
    return this.bigQuery;
  }

  public void setBigQuery(String bigQuery) {
    this.bigQuery = bigQuery;
  }

  public List<Tokens> getTokens() {
    return this.tokens;
  }

  public void setTokens(List<Tokens> tokens) {
    this.tokens = tokens;
  }
}
