package parser;

import java.util.List;

/**
 * Helper class that contains a node and all its neighbors
 */
public class Dependency {

  /* the current node */
  private String node;

  /* the possible neighbors to the current node */
  private List<String> neighbors;

  public String getNode() {
    return this.node;
  }

  public void setNode(String node) {
    this.node = node;
  }

  public List<String> getNeighbors() {
    return this.neighbors;
  }

  public void setNeighbors(List<String> neighbors) {
    this.neighbors = neighbors;
  }
}
