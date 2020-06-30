package com.google.cloud.bigquery.utils.autoqueryfixer.entity;

/**
 * A interface for queries' tokens.
 * */
public interface IToken {

  int getKind();

  String getImage();

  int getBeginLine();

  int getBeginCol();

  int getEndLine();

  int getEndCol();

}
