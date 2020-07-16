package com.google.cloud.bigquery.utils.queryfixer.entity;

import lombok.Value;

@Value
public class Position {
  int row;
  int column;
}
