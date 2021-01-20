package com.google.cloud.bigquery.utils.queryfixer.entity;

import lombok.Value;

@Value(staticConstructor = "of")
public class FixOption {

  // Action to fix a query. Usually, if an identifier is to be modified, this
  // field will present the new identifier to replace. For example, if a table is to be replaced,
  // the action looks like: Change to `new_table`.
  String action;

  // The fixed query in this option.
  String fixedQuery;
}
