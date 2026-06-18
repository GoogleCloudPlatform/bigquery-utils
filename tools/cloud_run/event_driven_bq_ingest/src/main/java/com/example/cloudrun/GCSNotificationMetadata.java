package com.example.cloudrun;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class GCSNotificationMetadata extends GenericMessage{
    private String name;
    private String bucket;

    public GCSNotificationMetadata() {}

    public GCSNotificationMetadata(@JsonProperty(value = "name", required = true) String name, @JsonProperty(value = "bucket", required = true) String bucket) {
        this.name = name;
        this.bucket = bucket;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public GCSObjectProperties getGCSObjectProperties() {
        String bucketId = this.bucket;
        String objectId = this.name;

        // return null if there is no objectId
        if (objectId == null || bucketId == null) {
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

        return GCSObjectProperties.builder()
                .bucketId(bucketId)
                .project(project)
                .dataset(dataset)
                .table(table)
                .triggerFile(triggerFileName)
                .build();
    }

    @Value
    @Builder
    public static class GCSObjectProperties {
        String bucketId;
        String project;
        String dataset;
        String table;
        String triggerFile;
    }
}
