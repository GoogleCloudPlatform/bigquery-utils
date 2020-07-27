package com.google.bigquery;

import static org.junit.Assert.*;

import javax.management.Query;
import org.junit.Test;

public class ErrorRecoveryTest {
  @Test
  public void deletionSingleLine() {
    String query = "SELECT GROUP a FROM A";
    assertEquals("SELECT a FROM A",
        QueryBreakdown.deletion(query, 1, 8, 12));
  }

  @Test
  public void deletionSingleLineNoSpace() {
    String query = "SELECT a FROM A WHERE a>GROUP";
    assertEquals("SELECT a FROM A WHERE a>",
        QueryBreakdown.deletion(query, 1, 25, 29));
  }

  @Test
  public void deletionMultipleLines() {
    String query = "SELECT a FROM A;" + '\n' + "SELECT GROUP b FROM B";
    assertEquals("SELECT a FROM A;" + '\n' + "SELECT b FROM B",
        QueryBreakdown.deletion(query, 2, 8, 12));
  }

  @Test
  public void deletionMultipleLinesNoSpace() {
    String query = "SELECT a FROM A;" + '\n' + "SELECT b FROM B WHERE b>GROUP";
    assertEquals("SELECT a FROM A;" + '\n' + "SELECT b FROM B WHERE b>",
        QueryBreakdown.deletion(query, 2, 25, 29));
  }
}