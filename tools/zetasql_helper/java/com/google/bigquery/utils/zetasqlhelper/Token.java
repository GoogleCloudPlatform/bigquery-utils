package com.google.bigquery.utils.zetasqlhelper;

import lombok.Value;

/**
 * A class representing the ZetaSQL tokens.
 */
@Value
public class Token {

    String image;
    Kind kind;

    int startByteOffset;
    int endByteOffset;

    public enum Kind {
        KEYWORD,               // A zetasql keyword or symbol.
        IDENTIFIER,            // An identifier that was quoted.
        IDENTIFIER_OR_KEYWORD,  // An unquoted identifier.
        VALUE,                 // A literal value.
        COMMENT,           // A comment.
        END_OF_INPUT,       // The end of the input string was reached.
    }

    /**
     * Create a Token object from the Token Protobuf.
     *
     * @param tokenProto Token Protobuf
     * @return Token instance
     */
    static Token fromPb(ParseToken.ParseTokenProto tokenProto) {
        int start = tokenProto.getParseLocationRange().getStart();
        int end = tokenProto.getParseLocationRange().getEnd();

        return new Token(tokenProto.getImage(), convertKind(tokenProto.getKind()),
                start, end);
    }

    private static Kind convertKind(ParseToken.ParseTokenProto.Kind kind) {
        switch (kind) {
            case KEYWORD:
                return Kind.KEYWORD;
            case IDENTIFIER:
                return Kind.IDENTIFIER;
            case IDENTIFIER_OR_KEYWORD:
                return Kind.IDENTIFIER_OR_KEYWORD;
            case VALUE:
                return Kind.VALUE;
            case COMMENT:
                return Kind.COMMENT;
            case END_OF_INPUT:
                return Kind.END_OF_INPUT;
        }
        throw new RuntimeException("Unknown error during conversion.");
    }
}
