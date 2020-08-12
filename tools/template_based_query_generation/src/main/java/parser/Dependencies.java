package parser;

import java.util.List;

/**
 * Helper class that contains all Dependency(s) for JSON deserialization
 */
public class Dependencies {
  /* all dependencies */
  private List<Dependency> dependencies;

  public List<Dependency> getDependencies() {
    return this.dependencies;
  }

  public void setDependencies(List<Dependency> dependencies) {
    this.dependencies = dependencies;
  }
}
