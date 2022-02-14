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

import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import com.google.api.services.pubsub.model.PubsubMessage;

@RestController
@Log4j2
public class PipelineController {

  private static final String TRIGGER_FILE_NAME = "trigger.txt";
  private static final String FILE_FORMAT = "AVRO";

  @RequestMapping(value = "/", method = RequestMethod.POST)
  public ResponseEntity receiveMessage(@RequestBody PipelineRequestBody request) {
    // Get PubSub pubSubMessage from request body.
    PubsubMessage pubSubMessage = request.getMessage();
    if (pubSubMessage == null) {
      log.info("Bad Request: invalid Pub/Sub pubSubMessage format");
      return new ResponseEntity("invalid Pub/Sub pubSubMessage", HttpStatus.BAD_REQUEST);
    }
    try {
      PubSubMessageProperties pubSubMessageProperties =
          PubSubMessageParser.parsePubSubProperties(pubSubMessage);
      if (pubSubMessageProperties == null) {
        // parse pubsub message as bq job notification
        PubSubMessageData pubSubMessageData =
            PubSubMessageParser.parsePubSubData(pubSubMessage.getData());
        List<String> sourceUris = JobAccessor.checkJobCompeletion(pubSubMessageData);
        sourceUris.forEach((sourceUri -> GCSAccessor.archiveFiles(sourceUri)));
        return new ResponseEntity("job completed", HttpStatus.OK);
      } else {
        // pubsub message was a gcs notification
        if (TRIGGER_FILE_NAME.equals(pubSubMessageProperties.getTriggerFile())) {
          log.info("Found Trigger file, started BQ insert");
          BQAccessor.insertIntoBQ(pubSubMessageProperties, FILE_FORMAT);
          // GCSAccessor.archiveFiles(pubSubMessageProperties);
          return new ResponseEntity("triggered successfully", HttpStatus.OK);
        } else {
          log.info("Not trigger file");
          return new ResponseEntity("Not trigger file", HttpStatus.OK);
        }
      }
    } catch (RuntimeException | JsonProcessingException e) {
      log.error("failed to process the message", e);
      return new ResponseEntity(e.getMessage(), HttpStatus.OK);
    }
  }
}
