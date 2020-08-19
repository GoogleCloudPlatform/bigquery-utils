package parser;

import java.util.List;

/**
 * Helper class that contains a node and all its neighbors
 */
public class Dependency {

  /* the current node */
  private FeatureType node;

  /* the possible neighbors to the current node */
  private List<FeatureType> neighbors;

  public FeatureType getNode() {
    return this.node;
  }

  public void setNode(FeatureType node) {
    this.node = node;
  }

  public List<FeatureType> getNeighbors() {
    return this.neighbors;
  }

  public void setNeighbors(List<FeatureType> neighbors) {
    this.neighbors = neighbors;
  }
}
