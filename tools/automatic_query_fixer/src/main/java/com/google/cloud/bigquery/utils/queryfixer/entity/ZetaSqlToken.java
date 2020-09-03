package com.google.cloud.bigquery.utils.queryfixer.entity;

import com.google.bigquery.utils.zetasqlhelper.Token;
import com.google.cloud.bigquery.utils.queryfixer.QueryPositionConverter;
import com.google.cloud.bigquery.utils.queryfixer.util.ByteOffsetTranslator;

/** A token class that is built on top of the Token defined in ZetaSQL. */
public class ZetaSqlToken implements IToken {
  private final Token token;
  private final int startRow;
  private final int startColumn;
  private final int endRow;
  private final int endColumn;

  public ZetaSqlToken(Token token, int startRow, int startColumn, int endRow, int endColumn) {
    this.token = token;
    this.startRow = startRow;
    this.startColumn = startColumn;
    this.endRow = endRow;
    this.endColumn = endColumn;
  }

  public ZetaSqlToken(Token token, Position startPosition, Position endPosition) {
    this(
        token,
        startPosition.getRow(),
        startPosition.getColumn(),
        endPosition.getRow(),
        endPosition.getColumn());
  }

  @Override
  public Kind getKind() {
    switch (token.getKind()) {
      case KEYWORD:
        return Kind.KEYWORD;
      case IDENTIFIER_OR_KEYWORD:
      case IDENTIFIER:
        return Kind.IDENTIFIER;
      case VALUE:
        return Kind.VALUE;
      case END_OF_INPUT:
        return Kind.END_OF_INPUT;
      default:
        return Kind.OTHERS;
    }
  }

  @Override
  public String getImage() {
    return token.getImage();
  }

  @Override
  public int getBeginRow() {
    return startRow;
  }

  @Override
  public int getBeginColumn() {
    return startColumn;
  }

  @Override
  public int getEndRow() {
    return endRow;
  }

  @Override
  public int getEndColumn() {
    return endColumn;
  }

  /** A factory to generate {@link ZetaSqlToken}. One factory is correspond to a query. */
  public static class Factory {
    private final ByteOffsetTranslator translator;
    private final QueryPositionConverter converter;

    public Factory(String query) {
      this.translator = ByteOffsetTranslator.of(query);
      this.converter = new QueryPositionConverter(query);
    }

    /**
     * Create a {@link ZetaSqlToken} from the token defined in ZetaSQL Helper client.
     *
     * @param token token from ZetaSQL Helper client.
     * @return a ZetaSqlToken
     */
    public ZetaSqlToken create(Token token) {
      int startIndex = translator.offsetToIndex(token.getStartByteOffset());
      Position startPosition = converter.indexToPos(startIndex);

      int endIndex = translator.offsetToIndex(token.getEndByteOffset());
      // End position should be included in Token, so -1 is used here.
      Position endPosition = converter.indexToPos(endIndex - 1);

      return new ZetaSqlToken(token, startPosition, endPosition);
    }
  }
}
