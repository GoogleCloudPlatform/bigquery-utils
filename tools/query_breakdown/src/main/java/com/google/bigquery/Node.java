package com.google.bigquery;

import java.util.ArrayList;

/**
 * This class represents a single node in the tree visualization of the algorithm. Below are fields
 * necessary for implementation.
 */
public class Node {

  // ensures tree structure

  private ArrayList<Node> children;
  private Node parent;

  // ensures that we know which location the error occurs in
  private int startLine;
  private int startColumn;
  private int endLine;
  private int endColumn;

  // ensures that we know how the error was handled
  private String replaceFrom;
  private String replaceTo;

  // ensures that we know what the performance measure is and how our current path is doing
  private int unparseableCount;

  // indicates how we handled the error
  enum errorHandlingType {
    DELETION,
    REPLACEMENT
  }

  /**
   * constructor for the node class
   */
  public Node (Node parent, int startLine, int startColumn, int endLine, int endColumn,
      boolean errorHandlingType, String replaceFrom, String replaceTo, int unparseableCount) {
  }
}
