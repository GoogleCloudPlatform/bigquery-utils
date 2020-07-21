import parser.Mapping;
import token.Token;

/**
 * class representing Query
 */
public class Query {

  private FeatureType type;
  private Mapping mapping;
  private Token token;

  public Query(FeatureType type) {
      this.type = type;
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

  public FeatureType getType() {
      return this.type;
  }

  public void setType(FeatureType type) {
    this.type = type;
  }

  /**
   *
   * @return whether FeatureType type is kind of query (not one of ROOT, DDL, DML, DQL)
   */
  public boolean isQuery() {
    return !(this.type == FeatureType.ROOT ||
        this.type == FeatureType.DDL ||
        this.type == FeatureType.DML ||
        this.type == FeatureType.DQL);
  }

}