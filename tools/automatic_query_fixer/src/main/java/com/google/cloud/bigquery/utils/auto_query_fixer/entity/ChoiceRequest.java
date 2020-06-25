package com.google.cloud.bigquery.utils.auto_query_fixer.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class ChoiceRequest {

  private Object errPos;
  private List<Choice> choices;

  public ChoiceRequest() {
    choices = new ArrayList<>();
  }

  public void addChoice(Choice choice) {
    choices.add(choice);
  }
}
