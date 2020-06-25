package com.google.cloud.bigquery.utils.auto_query_fixer.entity;


public interface IToken {

  int getKind();

  String getImage();

  int getBeginLine();

  int getBeginCol();

  int getEndLine();

  int getEndCol();

}
