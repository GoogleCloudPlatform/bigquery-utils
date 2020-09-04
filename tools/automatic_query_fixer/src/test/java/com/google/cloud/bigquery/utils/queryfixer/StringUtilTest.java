package com.google.cloud.bigquery.utils.queryfixer;

import com.google.cloud.bigquery.utils.queryfixer.util.StringUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertEquals;

public class StringUtilTest {

  @Test
  public void testEditDistance() {
    String target = "Google";
    List<String> dict =
        ImmutableList.of(
            "google", "GooGle", "oogle", "Googe", "Gooogle", "Gogle", "Happy", "gogle", "oGogle");

    StringUtil.SimilarStrings similarStrings =
        StringUtil.findMostSimilarWords(dict, target, /*caseSensitive=*/ true);
    assertEquals(1, similarStrings.getDistance());
    assertEquals(6, similarStrings.getStrings().size());
    assertThat(
        similarStrings.getStrings(),
        contains("google", "GooGle", "oogle", "Googe", "Gooogle", "Gogle"));
  }

  @Test
  public void testReplacingString() {
    String string = "Hello World!";
    String replaced = StringUtil.replaceStringBetweenIndex(string, 6, 11, "Google Cloud");
    assertEquals("Hello Google Cloud!", replaced);
  }
}
