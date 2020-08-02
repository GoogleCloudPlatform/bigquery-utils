package parser;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import token.TokenInfo;
import token.TokenType;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeywordsMappingTest {

  @Test
  public void test_getMapping() {
    TokenInfo tokenInfo = new TokenInfo();
    tokenInfo.setCount(1);
    tokenInfo.setRequired(true);
    tokenInfo.setTokenType("partition_exp");
    List<TokenInfo> tokenInfos = new ArrayList<>();
    tokenInfos.add(tokenInfo);
    Mapping mapping = new Mapping();
    mapping.setPostgres("PARTITION BY");
    mapping.setBigQuery("PARTITION BY");
    mapping.setTokenInfos(tokenInfos);
    List<Mapping> mappings = new ArrayList<>();
    mappings.add(mapping);
    ImmutableList<Mapping> expected = ImmutableList.copyOf(mappings);

    KeywordsMapping keywordsMapping = new KeywordsMapping();
    ImmutableList<Mapping> actual = keywordsMapping.getMappingDDL("DDL_PARTITION");

    assertEquals(expected.get(0).getTokenInfos().get(0).getCount(), actual.get(0).getTokenInfos().get(0).getCount());
    assertEquals(expected.get(0).getTokenInfos().get(0).getRequired(), actual.get(0).getTokenInfos().get(0).getRequired());
    assertEquals(expected.get(0).getTokenInfos().get(0).getTokenType(), actual.get(0).getTokenInfos().get(0).getTokenType());
    assertEquals(expected.get(0).getPostgres(), actual.get(0).getPostgres());
    assertEquals(expected.get(0).getBigQuery(), actual.get(0).getBigQuery());

    assertThrows(IllegalArgumentException.class, () -> {
      keywordsMapping.getMappingDDL("NON KEYWORD");
    });
  }
}
