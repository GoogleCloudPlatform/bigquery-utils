package com.google.cloud.bigquery.utils.autoqueryfixer.entity;

import lombok.Value;

/**
 * An option to fix an error of a query. It contains the description on how to fix the error and the
 * query after fixing.
 */
@Value
public class FixOption {
  String description;
  String fixedQuery;
}
