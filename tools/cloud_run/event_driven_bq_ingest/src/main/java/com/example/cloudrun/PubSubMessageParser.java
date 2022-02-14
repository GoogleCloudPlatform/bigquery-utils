package com.example.cloudrun;

import java.util.Map;
import lombok.extern.log4j.Log4j2;
import java.util.Base64;
import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.services.pubsub.model.PubsubMessage;

@Log4j2
public class PubSubMessageParser {

  public static PubSubMessageProperties parsePubSubProperties(PubsubMessage message) {

    Map<String, String> attributes = message.getAttributes();
    if (attributes == null) {
      throw new RuntimeException("No attributes in the pubsub message");
    }

    String bucketId = attributes.get("bucketId");
    String objectId = attributes.get("objectId");

    // return null if there is no objectId
    if (objectId == null) {
      return null;
    }

    String[] parsedObjectId = objectId.split("/");

    if (parsedObjectId.length < 4) {
      throw new RuntimeException("The object id is formatted incorrectly");
    }
    String project = parsedObjectId[0];
    String dataset = parsedObjectId[1];
    String table = parsedObjectId[2];
    String triggerFileName = parsedObjectId[3];

    return PubSubMessageProperties.builder()
        .bucketId(bucketId)
        .project(project)
        .dataset(dataset)
        .table(table)
        .triggerFile(triggerFileName)
        .build();
  }

  public static PubSubMessageData parsePubSubData(String data) throws JsonProcessingException {
    String dataStr = !StringUtils.isEmpty(data) ? new String(Base64.getDecoder().decode(data)) : "";
    ObjectMapper mapper = new ObjectMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    PubSubMessageData dataObj = mapper.readValue(dataStr, PubSubMessageData.class);
    return dataObj;
  }
}
