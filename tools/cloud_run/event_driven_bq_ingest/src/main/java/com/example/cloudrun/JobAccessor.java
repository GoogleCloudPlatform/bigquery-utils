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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.ExternalTableDefinition;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.JobConfiguration;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;

@Log4j2
public class JobAccessor {

  public static List<String> checkJobCompeletion(BigQueryLogMetadata bigQueryLogMetadata) {
    log.info("Resource Name:{}", bigQueryLogMetadata.getProtoPayload().getResourceName());

    try {
      String resourceName = bigQueryLogMetadata.getProtoPayload().getResourceName();
      String[] parsedName = resourceName.split("/");
      String jobName = parsedName[parsedName.length - 1];

      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
      JobId jobId = JobId.of(jobName);
      Job job = bigquery.getJob(jobId);

      List<String> allSourceUris = new ArrayList<String>();
      // check for error
      if (job.isDone() && job.getStatus().getError() == null) {
        log.info("Job successfully loaded BQ table");
        JobConfiguration jobConfig = job.getConfiguration();
        if (jobConfig instanceof QueryJobConfiguration) {
          Map<String, ExternalTableDefinition> tableConfigs =
              ((QueryJobConfiguration) jobConfig).getTableDefinitions();
          tableConfigs.forEach(
              (tableKey, tableValue) ->
                  tableValue
                      .getSourceUris()
                      .forEach((item) -> allSourceUris.add(removeWildcard(item))));
          log.info("end of tables");
        } else {
          log.info("job configuration is not an instance");
        }
      } else {
        log.info(
            "BigQuery was unable to load into the table due to an error:"
                + job.getStatus().getError());
        throw new RuntimeException(
            "BigQuery was unable to load into the table due to an error: "
                + job.getStatus().getError().getMessage());
      }
      return allSourceUris;
    } catch (NullPointerException | BigQueryException e) {
      log.info("Job not retrieved. \n" + e.toString());
      return null;
    }
  }

  public static String removeWildcard(String str) {
    String[] arrOfStr = str.split("\\*", 0);
    return arrOfStr[0];
  }
}
