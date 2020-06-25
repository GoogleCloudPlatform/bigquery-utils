package com.google.cloud.bigquery.utils.auto_query_fixer.entity;


import lombok.Data;

@Data
public class FixResult {

  private Status status;
  private String output;
  private String error;


  private enum Status {
    SUCCESS, FAIL, REQUEST_RESPONSE
  }
}
