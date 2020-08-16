package com.google.cloud.bigquery.utils.queryfixer.entity;

import lombok.Value;

@Value(staticConstructor = "of")
public class FixOption {

  // A short description about this fixing option. Usually, if an identifier is to be modified, this
  // field will be the new identifier to replace. For example, if a table is to be replaced, this
  // field will be the new table name.
  String description;

  // The fixed query in this option.
  String fixedQuery;
}
