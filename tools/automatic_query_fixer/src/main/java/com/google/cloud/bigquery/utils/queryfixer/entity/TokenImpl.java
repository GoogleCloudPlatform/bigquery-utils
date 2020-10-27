package com.google.cloud.bigquery.utils.queryfixer.entity;

import org.apache.calcite.sql.parser.babel.Token;

/**
 * An implementation of token interface based on the Token class of Babel Calcite Parser.
 * */
public class TokenImpl implements IToken {

  private final Token token;

  public TokenImpl (Token token) {
    this.token = token;
  }

  @Override public Kind getKind() {
    if (token.kind == 786 || token.kind == 788) {
      return Kind.IDENTIFIER;
    }
    return Kind.OTHERS;
  }

  @Override public String getImage() {
    return token.image;
  }

  @Override public int getBeginRow() {
    return token.beginLine;
  }

  @Override public int getBeginColumn() {
    return token.beginColumn;
  }

  @Override public int getEndRow() {
    return token.endLine;
  }

  @Override public int getEndColumn() {
    return token.endColumn;
  }

  // babel is a type of parser in Calcite, and this class uses its token for implementation.
  public Token getBabelToken() {
    return token;
  }

  @Override public String toString() {
    return  String.format("%s [%d:%d]", token.image, token.beginLine, token.beginColumn);
  }
}
