package parser;

import java.util.List;

/**
 * Helper class that contains all keyword variants of a feature
 */
public class Feature {
  private String feature;
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
