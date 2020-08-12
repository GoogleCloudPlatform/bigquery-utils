package query;

import com.google.common.collect.ImmutableList;
import parser.*;
import token.Token;
import token.TokenInfo;
import token.Tokenizer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * keyword parser that adds token placeholders to randomized keywords
 */
public class Skeleton {

  private final Keywords keywords = new Keywords();

  private final KeywordsMapping keywordsMapping = new KeywordsMapping();

  private Map<String, List<String>> dialectSkeletons = new HashMap<>();

  private final String filePathUser = "./src/main/resources/user_config/config.json";
  private final User user = Utils.getUser(Paths.get(filePathUser));

  /**
   * Constructor of randomized keyword parser that splices token placeholders with generated keywords
   */
  // TODO (spoiledhua): change input and output to Query Objects
  public Skeleton(List<Query> rawQueries, Tokenizer tokenizer) throws IOException {

    for (String dialect : user.getDialectIndicators().keySet()) {
      if (user.getDialectIndicators().get(dialect)) {
        dialectSkeletons.put(dialect, new ArrayList<>());
      }
    }

    for (Query rawQuery : rawQueries) {
      ImmutableList<Mapping> mappingList = getLanguageMap(rawQuery.getType().name());

      // choose a random variant from the list of possible keyword variants
      int randomIndex = Utils.getRandomInteger(mappingList.size() - 1);
      Mapping keywordVariant = mappingList.get(randomIndex);

      for (String dialect : user.getDialectIndicators().keySet()) {
        if (user.getDialectIndicators().get(dialect)) {
          dialectSkeletons.get(dialect).add(keywordVariant.getDialectMap().get(dialect));
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
              dialectSkeletons.get(dialect).add(token.getDialectExpressions().get(dialect));
            } else if (Utils.getRandomInteger(1) == 1) {
              dialectSkeletons.get(dialect).add(token.getDialectExpressions().get(dialect));
            }
          }
        }
      }
    }
  }

  /**
   * Gets mappings between dialects and the appropriate skeletons
   */
  public Map<String, List<String>> getDialectSkeletons() {
    return dialectSkeletons;
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
