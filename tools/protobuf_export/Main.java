package proto;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import path.to.proto.TestMessage;
import java.util.UUID;

/** Queries Google BigQuery */
public final class Main {
  public static void main(String[] args) throws Exception {
    String projectId = "your-project-id";
    BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();

    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
                " SELECT "
                    + "concat(word , \":\",corpus) as RowKey,"
                    + "<dataset-id>.toMyProtoMessage(STRUCT(word, "
                    + "CAST(word_count AS BIGNUMERIC))) AS ProtoResult "
                    + "FROM "
                    + "`bigquery-public-data.samples.shakespeare` "
                    + "ORDER BY word_count DESC "
                    + "LIMIT 20")
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    TableResult result = queryJob.getQueryResults();

    // Print all pages of the results.
    for (FieldValueList row : result.iterateAll()) {
      String key = row.get("RowKey").getStringValue();
      byte[] message = row.get("ProtoResult").getBytesValue();
      TestMessage testMessage = TestMessage.parseFrom(message);
      System.out.printf("rowKey: %s, message: %s\n", key, testMessage);
    }
  }
}
