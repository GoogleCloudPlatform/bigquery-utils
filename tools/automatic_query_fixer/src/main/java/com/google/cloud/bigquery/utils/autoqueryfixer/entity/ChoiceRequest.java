package com.google.cloud.bigquery.utils.autoqueryfixer.entity;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * A request to ask users to choose/confirm an option to fix the query.
 * The request contains error position and a list of fix options.
 * @see package com.google.cloud.bigquery.utils.auto_query_fixer.entity.FixOption
 */

@Data
public class ChoiceRequest {

  Object errPos;
  List<FixOption> fixOptions;

  /**
   * create a choiceRequest and initialize its fixOptions List.
   * */
  public ChoiceRequest() {
    fixOptions = new ArrayList<>();
  }

  public void addChoice(FixOption fixOption) {
    fixOptions.add(fixOption);
  }
}
