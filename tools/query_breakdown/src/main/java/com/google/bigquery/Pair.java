package com.google.bigquery;

/**
 * The Pair class is designed to keep the (x, y) coordinates of a token throughout the tool
 */
public class Pair {
  private final int x;
  private final int y;

  public Pair(int x, int y) {
    this.x = x;
    this.y = y;
  }

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!Pair.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    Pair p = (Pair) obj;
    if (p.getX() == x && p.getY() == y) {
      return true;
    }
    else {
      return false;
    }
  }
}
