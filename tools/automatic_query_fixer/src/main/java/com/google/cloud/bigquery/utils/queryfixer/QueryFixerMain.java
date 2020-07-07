package com.google.cloud.bigquery.utils.queryfixer;

import com.google.common.flogger.FluentLogger;

public class QueryFixerMain {

  private static final FluentLogger logger = FluentLogger.forEnclosingClass();

  public static void main(String[] args) {

    if (args.length == 0) {
      logger.atInfo().log("not enough arguments");
      return;
    }

    // TODO: this should act as a command line, which will be developed after the fixer component is ready.
  }
}
