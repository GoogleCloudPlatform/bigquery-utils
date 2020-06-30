package com.google.cloud.bigquery.utils.autoqueryfixer.entity;


import lombok.Data;

/**
 * A result on fixing a query. It has three states: SUCCESS, FAIL, REQUEST_RESPONSE.
 * SUCCESS means the fixed query is error prone. FAIL means the query contains error(s) and cannot be fixed.
 * REQUEST_RESPONSE means the auto fixer needs a response to perform next fixing.
 * */
@Data
public class FixResult {

  private Status status;
  private String output;
  private String error;


  private enum Status {
    SUCCESS, FAIL, REQUEST_RESPONSE
  }
}
