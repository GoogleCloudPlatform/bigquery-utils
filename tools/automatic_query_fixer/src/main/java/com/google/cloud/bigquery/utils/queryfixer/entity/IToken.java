package com.google.cloud.bigquery.utils.queryfixer.entity;

/**
 * An interface for queries' tokens.
 * */
public interface IToken {

  int getKind();

  String getImage();

  int getBeginRow();

  int getBeginColumn();

  int getEndRow();

  int getEndColumn();

}
