package com.google.cloud.bigquery.utils.auto_query_fixer.entity;


import lombok.Data;

@Data
public class Choice {
  private String description;
  private String fixedQuery;
}
