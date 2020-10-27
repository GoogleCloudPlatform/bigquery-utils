package com.google.cloud.bigquery.utils.queryfixer.entity;

/**
 * An interface for queries' tokens.
 * */
public interface IToken {

  Kind getKind();

  String getImage();

  int getBeginRow();

  int getBeginColumn();

  int getEndRow();

  int getEndColumn();

  enum Kind {
    KEYWORD,
    IDENTIFIER,
    VALUE,
    END_OF_INPUT,
    OTHERS
  }
}
