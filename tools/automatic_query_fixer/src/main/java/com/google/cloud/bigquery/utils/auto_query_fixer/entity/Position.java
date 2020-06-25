package com.google.cloud.bigquery.utils.auto_query_fixer.entity;

import lombok.Value;

@Value
public class Position {
  int line;
  int col;

  static public Position invalid() {
    return new Position(-1, -1);
  }

  public boolean isValid() {
    return line < 0 || col < 0;
  }
}
