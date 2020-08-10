package parser;

/**
 * Helper class that indicates whether a feature is included by the user via the user-defined config file
 */
public class FeatureIndicator {

  /* name of feature */
  private FeatureType feature;

  /* indicates whether the user would like the feature to be included */
  private boolean isIncluded;

  public FeatureType getFeature() {
    return this.feature;
  }

  public void setFeature(String feature) {
    this.feature = FeatureType.valueOf(feature);
  }

  public void setFeature(FeatureType feature) {
    this.feature = feature;
  }

  public boolean getIsIncluded() {
    return this.isIncluded;
  }

  public void setIsIncluded(boolean isIncluded) {
    this.isIncluded = isIncluded;
  }
}
