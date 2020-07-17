import com.google.common.collect.ImmutableList;
import parser.KeywordsMapping;

/**
 * keyword parser that adds token placeholders to randomized keywords
 */
public class Skeleton {

  private final KeywordsMapping keywordsMapping = new KeywordsMapping();

  private final ImmutableList<ImmutableList<String>> rawKeywordsLists;

  /**
   * Constructor of randomized keyword parser that splices token placeholders with generated keywords
   */
  public Skeleton(ImmutableList<ImmutableList<String>> rawKeywordsLists) {
    this.rawKeywordsLists = rawKeywordsLists;
  }

  /**
   * Creates strings of skeleton PostgreSQL statements from generated keywords
   *
   * @return a list of skeleton PostgreSQL statements
   */
  public ImmutableList<String> postgreSkeleton() {
    ImmutableList.Builder<String> builder = ImmutableList.builder();

    for (ImmutableList<String> rawKeywordList : rawKeywordsLists) {
      for (String rawKeyword : rawKeywordList) {
        // TODO (spoiledhua): splice token placeholders given raw keywords using keywords mappings (Postgres)
      }
    }
    return null;
  }

  /**
   *
   */
  public ImmutableList<String> BigQuerySkeleton() {
    ImmutableList.Builder<String> builder = ImmutableList.builder();

    for (ImmutableList<String> rawKeywordList : rawKeywordsLists) {
      for (String rawKeyword : rawKeywordList) {
        // TODO (spoiledhua): splice token placeholders given raw keywords using keywords mappings (BQ)
      }
    }
    return null;
  }
}
