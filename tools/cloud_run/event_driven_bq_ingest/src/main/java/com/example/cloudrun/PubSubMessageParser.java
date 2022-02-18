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
  public static GenericMessage parseMessage(PubsubMessage message) {
    String data = message.getData();
    if (data == null) {
      return null;
    } else {
      String dataStr = !StringUtils.isEmpty(data) ? new String(Base64.getDecoder().decode(data)) : "";
      ObjectMapper mapper = new ObjectMapper();
      mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
      try {
        GenericMessage dataObj = mapper.readValue(dataStr, GCSNotificationMetadata.class);

        return dataObj;
      } catch (JsonProcessingException error1) {
        try {
          GenericMessage dataObj = mapper.readValue(dataStr, BigQueryLogMetadata.class);
          return dataObj;
        } catch (JsonProcessingException error2) {
          log.error("failed to parse GCS Notification metadata", error1);
          log.error("failed to parse BQ Log metadata", error2);
          return null;
        }
      }
    }
  }
}
