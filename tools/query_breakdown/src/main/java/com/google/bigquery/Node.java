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
  enum ErrorHandlingType {
    DELETION,
    REPLACEMENT
  }

  ErrorHandlingType type;

  /**
   * constructor for the node class for deletion
   */
  public Node (Node parent, int startLine, int startColumn, int endLine, int endColumn,
      int unparseableCount) {
    this.parent = parent;
    this.startLine = startLine;
    this.startColumn = startColumn;
    this.endLine = endLine;
    this.endColumn = endColumn;
    type = ErrorHandlingType.DELETION;
    this.unparseableCount = unparseableCount;
  }

  /**
   * constructor for the node class for replacement
   */
  public Node (Node parent, int startLine, int startColumn, int endLine, int endColumn,
      String replaceFrom, String replaceTo, int unparseableCount) {
    this.parent = parent;
    this.startLine = startLine;
    this.startColumn = startColumn;
    this.endLine = endLine;
    this.endColumn = endColumn;
    type = ErrorHandlingType.REPLACEMENT;
    this.replaceFrom = replaceFrom;
    this.replaceTo = replaceTo;
    this.unparseableCount = unparseableCount;
  }

  /**
   * Override toString method for better debugging
   */
  @Override
  public String toString() {
    return "Parent: " + parent.getStartColumn() + parent.getEndColumn()
        + "\nstartLine: " + startLine + "\nstartColumn: " + startColumn + "\nendLine: " + endLine
    + "\nendColumn: " + endColumn + "\ntype: " + type + "\nreplaceFrom: " + replaceFrom +
        "\nreplaceTo: " + replaceTo + "\nunparseableCount: " + unparseableCount;
  }

  /**
   * no args constructor for the root node
   */
  public Node () {}

  /**
   * getters for fields
   */
  public Node getParent() {
    return parent;
  }

  public int getStartLine() {
    return startLine;
  }

  public int getStartColumn() {
    return startColumn;
  }

  public int getEndLine() {
    return endLine;
  }

  public int getEndColumn() {
    return endColumn;
  }

  public String getReplaceFrom() {
    return replaceFrom;
  }

  public String getReplaceTo() {
    return replaceTo;
  }

  public String getErrorHandlingType() {
    return type.toString();
  }
}
