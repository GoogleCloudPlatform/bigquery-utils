/*
 * Copyright 2021 Google LLC
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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.util.Arrays;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class GCSAccessor {

  private static final Storage storage = StorageOptions.getDefaultInstance().getService();

  public static void archiveFiles(String sourceUri) {

    String sourceBucketName = getBucketName(sourceUri);
    String targetBucketName = sourceBucketName + "_archival";
    String sourceObjectName = getObjectName(sourceUri);
    log.info("source uri: {}", sourceUri);
    log.info("sourceBucketName: {}", sourceBucketName);
    log.info("targetBucketName: {}", targetBucketName);
    log.info("sourceObjectName: {}", sourceObjectName);

    try {

      Page<Blob> blobs =
          storage.list(sourceBucketName, Storage.BlobListOption.prefix(sourceObjectName));
      log.info("Iterating the blobs");
      for (Blob blob : blobs.iterateAll()) {
        log.info("Blob is: " + blob.getName());
        if (!blob.getName().equalsIgnoreCase(sourceObjectName)) {
          CopyWriter copyWriter = blob.copyTo(targetBucketName, blob.getName());
          Blob copiedBlob = copyWriter.getResult();
        }
        blob.delete();
      }
    } catch (RuntimeException e) {
      log.error("failed to process the message", e);
      throw new RuntimeException("failed to archive files", e);
    }
  }

  public static String getBucketName(String filePath) {
    String[] path = filePath.replace("gs://", "").split("/");

    String bucket = path[0];
    return bucket;
  }

  public static String getObjectName(String filePath) {
    String[] path = filePath.replace("gs://", "").split("/");
    String objectName = String.join("/", Arrays.copyOfRange(path, 1, path.length));

    return objectName;
  }
}
