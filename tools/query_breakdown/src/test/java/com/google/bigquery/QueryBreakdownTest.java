package com.google.bigquery;

import static org.junit.Assert.*;

import org.junit.Test;

public class QueryBreakdownTest {
  @Test
  public void findNthIndexOfDNE() {
    String test = "abcde";
    assertEquals(-1, QueryBreakdown.findNthIndexOf(test, 'f', 1));
  }

  @Test
  public void findNthIndexOfFirst() {
    String test = "abcde";
    assertEquals(2, QueryBreakdown.findNthIndexOf(test, 'c', 1));
  }

  @Test
  public void findNthIndexOfMultiple() {
    String test = "abcddde";
    assertEquals(5, QueryBreakdown.findNthIndexOf(test, 'd', 3));
  }

  @Test
  public void findNthIndexOfMultipleDNE() {
    String test = "abcddde";
    assertEquals(-1, QueryBreakdown.findNthIndexOf(test, 'd', 4));
  }

  @Test
  public void findNthIndexOfMultipleNewLines() {
    String test = "abc\nd\ne";
    assertEquals(3, QueryBreakdown.findNthIndexOf(test, '\n', 1));
    assertEquals(5, QueryBreakdown.findNthIndexOf(test, '\n', 2));
    assertEquals(-1, QueryBreakdown.findNthIndexOf(test, '\n', 3));
  }
}