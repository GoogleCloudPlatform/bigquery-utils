package com.google.cloud.bigquery.utils.queryfixer.exception;

/**
 * An exception that will be thrown during when {@link com.google.cloud.bigquery.utils.queryfixer.BigQueryParserFactory}
 * is used to create a parser.
 *
 * @see com.google.cloud.bigquery.utils.queryfixer.BigQueryParserFactory
 * */
public class ParserCreationException extends RuntimeException {
  public ParserCreationException(String message) {
    super(message);
  }
}
