import token.Token;

/**
 * class representing Query
 */
public class Query {

  private QueryType type;
  private Mapping mapping;
  private boolean special;
  private Token token;

  public Query(QueryType type, boolean special) {
      this.type = type;
      this.special = special;
  }

  public Mapping getMapping() {
    return this.mapping;
  }

  public void setMapping(Mapping mapping) {
    this.mapping = mapping;
  }

  public Token getToken() {
    return this.token;
  }

  public void setToken(Token token) {
    this.token = token;
  }

  public boolean getSpecial() {
    return this.special;
  }

  public void setSpecial(boolean special) {
    this.special = special;
  }

  public QueryType getType() {
      return this.type;
  }

  public void setType(QueryType type) {
    this.type = type;
  }

}