/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.cloudrun;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BQAccessor {

  private static final String EXTERNAL_TABLE_NAME = "externalTable";

  public static void insertIntoBQ(
          GCSNotificationMetadata.GCSObjectProperties GCSObjectProperties, String fileFormat) {

    // create sourceUri with format --> gs://bucket/project/dataset/table/table*
    String sourceUri =
        String.format(
            "gs://%s/%s/%s/%s/%s*",
            GCSObjectProperties.getBucketId(),
            GCSObjectProperties.getProject(),
            GCSObjectProperties.getDataset(),
            GCSObjectProperties.getTable(),
            GCSObjectProperties.getTable());
    log.info("source URI is: {}", sourceUri);

    try {

      // Initialize client that will be used to send requests. This client only needs to be created
      // once, and can be reused for multiple requests.
      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

      FormatOptions format = FormatOptions.of(fileFormat);

      ExternalTableDefinition externalTable =
          ExternalTableDefinition.newBuilder(sourceUri, format).build();

      log.info("external table config: {}", externalTable);

      String query =
          String.format(
              "INSERT INTO `%s.%s.%s` SELECT * FROM %s",
              GCSObjectProperties.getProject(),
              GCSObjectProperties.getDataset(),
              GCSObjectProperties.getTable(),
              EXTERNAL_TABLE_NAME);
      QueryJobConfiguration queryConfig =
          QueryJobConfiguration.newBuilder(query)
              .addTableDefinition(EXTERNAL_TABLE_NAME, externalTable)
              .build();
      log.info("query we fired: {}", query);
      JobInfo jobInfo = JobInfo.of(queryConfig);
      Job job = bigquery.create(jobInfo);
      JobId jobId = job.getJobId();
      log.info("job id: {}", jobId);

    } catch (Exception e) {
      throw new RuntimeException("Exception occured during insertion to BQ", e);
    }
  }
}
