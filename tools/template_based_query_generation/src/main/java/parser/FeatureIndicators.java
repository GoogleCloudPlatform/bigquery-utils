package parser;

import java.util.List;

/**
 * Helper class that contains all FeatureIndicator(s) for JSON deserialization
 */
public class FeatureIndicators {

  /* contains all possible features from DDL, DML, or DQL syntax */
  private List<FeatureIndicator> featureIndicators;

  public List<FeatureIndicator> getFeatureIndicators() {
    return this.featureIndicators;
  }

  public void setFeatureIndicators(List<FeatureIndicator> featureIndicators) {
    this.featureIndicators = featureIndicators;
  }
}
