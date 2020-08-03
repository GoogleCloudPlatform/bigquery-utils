package parser;

public class Pair<F, S> {
  public F first; //first member of pair
  public S second; //second member of pair

  public Pair(F first, S second) {
    this.first = first;
    this.second = second;
  }

  public String toString() {
    return "(" + first + ", " + second + ")";
  }

}
