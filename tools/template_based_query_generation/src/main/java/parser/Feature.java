package parser;

import java.util.Map;

/**
 * Helper class that contains all keyword variants of a feature
 */
public class Feature {

  /* Describes the intended feature, i.e. DDL_CREATE, DDL_SELECT, etc. */
  private String feature;

  /* All possible keyword variants and their PostgreSQL and BigQuery mappings and token requirements */
  private Map<String, String> dialectMap;

  public String getFeature() {
    return this.feature;
  }

  public void setFeature(String feature) {
    this.feature = feature;
  }

  public Map<String, String> getDialectMap() {
    return this.dialectMap;
  }

  public void setDialectMap(Map<String, String> dialectMap) {
    this.dialectMap = dialectMap;
  }
}
