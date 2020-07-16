import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeywordsMappingTest {

  @Test
  public void test_getMappingDDL() {
    KeywordsMapping keywordsMapping = new KeywordsMapping();
    TokenInfo tokenInfo = new TokenInfo();
    tokenInfo.setCount(1);
    tokenInfo.setRequired(true);
    tokenInfo.setTokenName("partition_exp");
    ArrayList<TokenInfo> tokenInfos = new ArrayList<TokenInfo>();
    tokenInfos.add(tokenInfo);
    Mapping mapping = new Mapping();
    mapping.setPostgre("PARTITION BY");
    mapping.setBigQuery("PARTITION BY");
    mapping.setTokens(tokenInfos);
    ArrayList<Mapping> mappings = new ArrayList<Mapping>();

    assertEquals(mappings, keywordsMapping.getMappingDDL("DDL_PARTITION"));
    assertThrows(IllegalArgumentException.class, () -> {
      keywordsMapping.getMappingDDL("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingDML() {
    KeywordsMapping keywordsMapping = new KeywordsMapping();
    TokenInfo tokenInfo = new TokenInfo();
    tokenInfo.setCount(1);
    tokenInfo.setRequired(true);
    tokenInfo.setTokenName("update_item");
    ArrayList<TokenInfo> tokenInfos = new ArrayList<TokenInfo>();
    tokenInfos.add(tokenInfo);
    Mapping mapping = new Mapping();
    mapping.setPostgre("SET");
    mapping.setBigQuery("SET");
    mapping.setTokens(tokenInfos);
    ArrayList<Mapping> mappings = new ArrayList<Mapping>();

    assertEquals(mappings, keywordsMapping.getMappingDML("DML_SET"));
    assertThrows(IllegalArgumentException.class, () -> {
      keywordsMapping.getMappingDML("NON KEYWORD");
    });
  }

  @Test
  public void test_getMappingDQL() {
    KeywordsMapping keywordsMapping = new KeywordsMapping();
    TokenInfo tokenInfo = new TokenInfo();
    tokenInfo.setCount(1);
    tokenInfo.setRequired(true);
    tokenInfo.setTokenName("select_exp");
    ArrayList<TokenInfo> tokenInfos = new ArrayList<TokenInfo>();
    tokenInfos.add(tokenInfo);
    Mapping mapping = new Mapping();
    mapping.setPostgre("SELECT");
    mapping.setBigQuery("SELECT");
    mapping.setTokens(tokenInfos);
    ArrayList<Mapping> mappings = new ArrayList<Mapping>();

    assertEquals(mappings, keywordsMapping.getMappingDQL("DQL_SELECT"));
    assertThrows(IllegalArgumentException.class, () -> {
      keywordsMapping.getMappingDQL("NON KEYWORD");
    });
  }
}
