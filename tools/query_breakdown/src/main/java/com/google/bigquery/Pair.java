package com.google.bigquery;

/**
 * The Pair class is designed to keep the (x, y) coordinates of a token throughout the tool
 */
public class Pair {
  private final int x;
  private final int y;

  // constructor
  public Pair(int x, int y) {
    this.x = x;
    this.y = y;
  }

  // getter methods
  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  // override equals method for better equality check between pair objects and testing/debugging
  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!Pair.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    Pair p = (Pair) obj;
    return p.getX() == x && p.getY() == y;
  }
}
