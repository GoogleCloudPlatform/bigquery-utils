package com.example.cloudrun;

import lombok.Builder;
import lombok.Value;

public class GCSNotificationMetadata extends GenericMessage{
    private String id;
    private String name;
    private String bucket;

    public GCSNotificationMetadata() {}

    public GCSNotificationMetadata(String id, String name, String bucket) {
        this.id = id;
        this.name = name;
        this.bucket = bucket;
    }

    public String getInsertId() {
        return id;
    }

    public void setInsertId(String insertId) {
        this.id = id;
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

    @Value
    @Builder
    public class GCSObjectProperties {
        String bucketId;
        String project;
        String dataset;
        String table;
        String triggerFile;
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
}
