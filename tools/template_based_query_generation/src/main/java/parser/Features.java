package parser;

import java.util.List;

/**
 * Helper class that contains all Feature(s) for JSON deserialization
 */
public class Features {
  /* all features */
  private List<Feature> features;

  public List<Feature> getFeatures() {
    return this.features;
  }

  public void setFeatures(List<Feature> features) {
    this.features = features;
  }
}
