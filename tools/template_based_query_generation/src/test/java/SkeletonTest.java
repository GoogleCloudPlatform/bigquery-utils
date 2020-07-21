import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SkeletonTest {

  @Test
  public void test_getPostgreSkeleton() {
    ImmutableList.Builder<String> keywordsBuilder = ImmutableList.builder();
    keywordsBuilder.add("DDL_CLUSTER");
    ImmutableList<String> rawKeywordsList = keywordsBuilder.build();

    ImmutableList.Builder<String> expectedBuilder = ImmutableList.builder();
    expectedBuilder.add("COLLATE");
    expectedBuilder.add("cluster_exp");
    ImmutableList<String> expected = expectedBuilder.build();

    Skeleton skeleton = new Skeleton(rawKeywordsList);
    ImmutableList<String> actual = skeleton.getPostgreSkeleton();

    assertEquals(expected, actual);
  }

  @Test
  public void test_getBigQuerySkeleton() {
    ImmutableList.Builder<String> keywordsBuilder = ImmutableList.builder();
    keywordsBuilder.add("DDL_CLUSTER");
    ImmutableList<String> rawKeywordsList = keywordsBuilder.build();

    ImmutableList.Builder<String> expectedBuilder = ImmutableList.builder();
    expectedBuilder.add("CLUSTER BY");
    expectedBuilder.add("cluster_exp");
    ImmutableList<String> expected = expectedBuilder.build();

    Skeleton skeleton = new Skeleton(rawKeywordsList);
    ImmutableList<String> actual = skeleton.getBigQuerySkeleton();

    assertEquals(expected, actual);
  }
}
