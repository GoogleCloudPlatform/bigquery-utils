package token;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class QueryRegexTest {

  @Test
  public void test_queryRegex() {
    // TODO: continue adding tests

    List<String> featureRegexList = new ArrayList<>();
    featureRegexList.add("SELECT {* |<expression_>[,<expression_>] |DISTINCT <expression_>[,<expression_>] }{EXCEPT <column_> |}" );
    featureRegexList.add("FROM <tableName>");
    featureRegexList.add("WHERE <condition_>");
    // featureRegexList.add("GROUP BY <"): requires alias
    featureRegexList.add("HAVING <condition_>");
    featureRegexList.add("ORDER BY {<column_>|<function_>}[,{<column_>|<function_>}]");
    QueryRegex qr;
    for (int i = 0; i < 3; i++) {
      qr = new QueryRegex(featureRegexList, 3);
      System.out.println(qr.getFlattenedQueries());
    }
  }
}
