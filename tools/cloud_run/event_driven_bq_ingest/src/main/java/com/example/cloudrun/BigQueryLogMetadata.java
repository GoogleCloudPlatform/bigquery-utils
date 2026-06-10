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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BigQueryLogMetadata extends GenericMessage {

  private String insertId;
  private ProtoPayload protoPayload;

  public BigQueryLogMetadata(
      @JsonProperty("insertId") String insertId,
      @JsonProperty(value = "protoPayload", required = true) ProtoPayload protoPayload) {
    this.insertId = insertId;
    this.protoPayload = protoPayload;
  }

  public String getInsertId() {
    return insertId;
  }

  public void setInsertId(String insertId) {
    this.insertId = insertId;
  }

  public ProtoPayload getProtoPayload() {
    return protoPayload;
  }

  public void setProtoPayload(ProtoPayload protoPayload) {
    this.protoPayload = protoPayload;
  }

  public static class ProtoPayload {
    private String resourceName;

    public ProtoPayload(
        @JsonProperty(value = "resourceName", required = true) String resourceName) {
      this.resourceName = resourceName;
    }

    public String getResourceName() {
      return resourceName;
    }

    public void setResourceName(String resourceName) {
      this.resourceName = resourceName;
    }
  }
}
