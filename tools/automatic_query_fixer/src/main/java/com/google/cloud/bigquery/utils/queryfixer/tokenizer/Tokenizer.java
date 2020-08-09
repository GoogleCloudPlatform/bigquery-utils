package com.google.cloud.bigquery.utils.queryfixer.tokenizer;

import com.google.cloud.bigquery.utils.queryfixer.entity.IToken;

import java.util.List;

public interface Tokenizer {

  List<IToken> tokenize(String query);
}
