import org.junit.jupiter.api.Test;

public class SkeletonTest {

  @Test
  public void test_getPostgreSkeleton() {
    // TODO (spoiledhua): refactor unit tests to reflect class changes
    /*
    ImmutableList.Builder<String> keywordsBuilder = ImmutableList.builder();
    keywordsBuilder.add("DDL_CLUSTER");
    ImmutableList<String> rawKeywordsList = keywordsBuilder.build();

    ImmutableList.Builder<String> expectedBuilder = ImmutableList.builder();
    expectedBuilder.add("COLLATE");
    expectedBuilder.add("cluster_exp");
    ImmutableList<String> expected = expectedBuilder.build();

    query.Skeleton skeleton = new query.Skeleton(rawKeywordsList);
    ImmutableList<String> actual = skeleton.getPostgreSkeleton();

    assertEquals(expected, actual);
     */
  }

  @Test
  public void test_getBigQuerySkeleton() {

    /*
    ImmutableList.Builder<String> keywordsBuilder = ImmutableList.builder();
    keywordsBuilder.add("DDL_CLUSTER");
    ImmutableList<String> rawKeywordsList = keywordsBuilder.build();

    ImmutableList.Builder<String> expectedBuilder = ImmutableList.builder();
    expectedBuilder.add("CLUSTER BY");
    expectedBuilder.add("cluster_exp");
    ImmutableList<String> expected = expectedBuilder.build();

    query.Skeleton skeleton = new query.Skeleton(rawKeywordsList);
    ImmutableList<String> actual = skeleton.getBigQuerySkeleton();

    assertEquals(expected, actual);
     */
  }
}
