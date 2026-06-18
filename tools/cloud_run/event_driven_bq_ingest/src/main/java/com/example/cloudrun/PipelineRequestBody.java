package com.example.cloudrun;

import com.google.api.services.pubsub.model.PubsubMessage;

public class PipelineRequestBody {
  private PubsubMessage message;

  public PipelineRequestBody() {}

  public PubsubMessage getMessage() {
    return message;
  }

  public void setMessage(PubsubMessage message) {
    this.message = message;
  }
}
