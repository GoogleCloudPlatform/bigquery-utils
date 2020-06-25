package com.google.cloud.bigquery.utils.auto_query_fixer;

public class Application {

  public static void main(String[] args) {

    if (args.length == 0 ) {
      System.out.println("not enough arguments");
      return;
    }

    String query = args[0];
  }
}
