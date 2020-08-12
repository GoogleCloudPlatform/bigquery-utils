package parser;

/**
 * Helper class that maps a feature or datatype to its appropriate dialect keyword
 */
public class DialectMap {

  /* Name of the dialect */
  String dialect;

  /* Mapping to a feature in the dialect */
  String mapping;

  public String getDialect() {
    return dialect;
  }

  public void setDialect(String dialect) {
    this.dialect = dialect;
  }

  public String getMapping() {
    return mapping;
  }

  public void setMapping(String mapping) {
    this.mapping = mapping;
  }
}
