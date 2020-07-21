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
   * @return whether FeatureType type is kind of query
   * not one of feature_root, ddl_feature_root, dml_feature_root, dql_feature_root, feature_sink
   */
  public boolean isQuery() {
    return !(this.type == FeatureType.feature_root ||
        this.type == FeatureType.ddl_feature_root ||
        this.type == FeatureType.dml_feature_root ||
        this.type == FeatureType.dql_feature_root ||
        this.type == FeatureType.feature_sink);
  }

}