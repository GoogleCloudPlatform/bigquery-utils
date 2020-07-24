package parser;

import java.util.List;

/**
 * Helper class that contains all keyword variants of a feature
 */
public class Feature {

  /* Describes the intended feature, i.e. DDL_CREATE, DDL_SELECT, etc. */
  private String feature;

  /* All possible keyword variants and their PostgreSQL and BigQuery mappings and token requirements */
  private List<Mapping> allMappings;

  public String getFeature() {
    return this.feature;
  }

  public void setFeature(String feature) {
    this.feature = feature;
  }

  public List<Mapping> getAllMappings() {
    return this.allMappings;
  }

  public void setAllMappings(List<Mapping> allMappings) {
    this.allMappings = allMappings;
  }
}
