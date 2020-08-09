package query;

import com.google.common.collect.ImmutableList;
import parser.Keywords;
import parser.KeywordsMapping;
import parser.Mapping;
import parser.Utils;
import token.Token;
import token.TokenInfo;
import token.Tokenizer;

import java.util.ArrayList;
import java.util.List;

/**
 * keyword parser that adds token placeholders to randomized keywords
 */
public class Skeleton {

  private final Keywords keywords = new Keywords();

  private final KeywordsMapping keywordsMapping = new KeywordsMapping();

  private final ImmutableList<String> postgreSkeleton;
  private final ImmutableList<String> bigQuerySkeleton;

  /**
   * Constructor of randomized keyword parser that splices token placeholders with generated keywords
   */
  // TODO (spoiledhua): change input and output to query.Query Objects
  public Skeleton(List<Query> rawQueries, Tokenizer tokenizer) {
    ImmutableList.Builder<String> postgresBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> bigQueryBuilder = ImmutableList.builder();

    for (Query rawQuery : rawQueries) {
      ImmutableList<Mapping> mappingList = getLanguageMap(rawQuery.getType().name());

      // choose a random variant from the list of possible keyword variants
      int randomIndex = Utils.getRandomInteger(mappingList.size() - 1);
      Mapping keywordVariant = mappingList.get(randomIndex);
      postgresBuilder.add(keywordVariant.getDialectMap().get("postgres"));
      bigQueryBuilder.add(keywordVariant.getDialectMap().get("bigQuery"));
      List<TokenInfo> tokenInfos = keywordVariant.getTokenInfos();

      List<Token> tokens = new ArrayList<>();
      for (TokenInfo tokenInfo : tokenInfos) {
        Token token = new Token(tokenInfo);
        tokens.add(token);
      }

      rawQuery.setTokens(tokens);
      for (Token token : tokens) {
        tokenizer.generateToken(token);
        if (token.getTokenInfo().getRequired()) {
          postgresBuilder.add(token.getPostgresTokenExpression());
          bigQueryBuilder.add(token.getBigQueryTokenExpression());
        } else if (Utils.getRandomInteger(1) == 1) {
          postgresBuilder.add(token.getPostgresTokenExpression());
          bigQueryBuilder.add(token.getBigQueryTokenExpression());
        }
      }
    }

    postgreSkeleton = postgresBuilder.build();
    bigQuerySkeleton = bigQueryBuilder.build();
  }

  /**
   * Gets strings of skeleton PostgreSQL statements from generated keywords
   *
   * @return a list of skeleton PostgreSQL statements
   */
  public ImmutableList<String> getPostgreSkeleton() {
    return postgreSkeleton;
  }

  /**
   * Gets strings of skeleton BigQuery statements from generated keywords
   *
   * @return a list of skeleton BigQuery statements
   */
  public ImmutableList<String> getBigQuerySkeleton() {
    return bigQuerySkeleton;
  }

  /**
   * Fetches the appropriate DDL, DML, or DQL keyword mapping
   *
   * @param rawKeyword the keyword to categorize
   * @return the list of mappings associated with the keyword
   */
  private ImmutableList<Mapping> getLanguageMap(String rawKeyword) {
    if (keywords.inKeywordsDDL(rawKeyword)) {
      return keywordsMapping.getMappingDDL(rawKeyword);
    } else if (keywords.inKeywordsDML(rawKeyword)) {
      return keywordsMapping.getMappingDML(rawKeyword);
    } else {
      return keywordsMapping.getMappingDQL(rawKeyword);
    }
  }
}
