import com.google.common.collect.ImmutableList;
import parser.Keywords;
import parser.KeywordsMapping;
import parser.Mapping;
import parser.Utils;
import token.TokenInfo;

import java.util.List;

/**
 * keyword parser that adds token placeholders to randomized keywords
 */
public class Skeleton {

  private final Keywords keywords = new Keywords();

  private final KeywordsMapping keywordsMapping = new KeywordsMapping();

  private ImmutableList<String> postgreSkeleton = new ImmutableList.Builder<String>().build();
  private ImmutableList<String> bigQuerySkeleton = new ImmutableList.Builder<String>().build();

  /**
   * Constructor of randomized keyword parser that splices token placeholders with generated keywords
   */
  // TODO (spoiledhua): change input and output to Query Objects
  public Skeleton(ImmutableList<String> rawKeywordsList) {
    ImmutableList.Builder<String> postgresBuilder = ImmutableList.builder();
    ImmutableList.Builder<String> bigQueryBuilder = ImmutableList.builder();

    for (String rawKeyword : rawKeywordsList) {
      ImmutableList<Mapping> mappingList = getLanguageMap(rawKeyword);

      // choose a random variant from the list of possible keyword variants
      int randomIndex = Utils.getRandomInteger(mappingList.size() - 1);
      Mapping keywordVariant = mappingList.get(randomIndex);
      postgresBuilder.add(keywordVariant.getPostgres());
      bigQueryBuilder.add(keywordVariant.getBigQuery());
      List<TokenInfo> tokens = keywordVariant.getTokenInfos();

      for (TokenInfo token : tokens) {
        // if token is required, add it to the skeleton, otherwise add it with a 1/2 probability
        if (token.getRequired()) {
          postgresBuilder.add(token.getTokenName());
          bigQueryBuilder.add(token.getTokenName());
        } else if (Utils.getRandomInteger(1) == 1) {
          postgresBuilder.add(token.getTokenName());
          bigQueryBuilder.add(token.getTokenName());
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
