/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.firehose;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.CreateDeliveryStreamRequest;
import software.amazon.awssdk.services.firehose.model.DeliveryStreamType;
import software.amazon.awssdk.services.firehose.model.ExtendedS3DestinationConfiguration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class FirehoseResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("localstack/localstack");
    private static final LocalStackContainer localStackContainer = new LocalStackContainer(DEFAULT_IMAGE_NAME)
            .withServices(LocalStackContainer.Service.FIREHOSE, LocalStackContainer.Service.S3, LocalStackContainer.Service.STS);

    private static final AtomicBoolean running = new AtomicBoolean(false);

    public static final String FIREHOSE_BUCKET = "it-s3-bucket";
    public static final String STREAM_NAME = "testc.inventory.customers";

    private static synchronized void init() throws java.io.IOException, InterruptedException {
        if (!running.get()) {
            localStackContainer.start();
            running.set(true);
        }
    }

    @Override
    public Map<String, String> start() {
        try {
            init();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start LocalStack container", e);
        }

        // Initialize parameters for the test environment
        Map<String, String> params = new ConcurrentHashMap<>();
        params.put("debezium.sink.firehose.stream.name", STREAM_NAME);
        params.put("debezium.sink.firehose.region", getAWSRegion());
        params.put("debezium.sink.firehose.endpoint", getAWSEndpoint().toString());

        // Create S3 bucket and Firehose stream
        createS3Bucket(s3Client());
        createFirehoseStream();

        return params;
    }

    private static void createFirehoseStream() {
        ExtendedS3DestinationConfiguration s3DestinationConfiguration = ExtendedS3DestinationConfiguration
                .builder().bucketARN("arn:aws:s3:::" + FIREHOSE_BUCKET)
                .roleARN("arn:aws:iam::000000000000:role/FirehoseDeliveryRole") // LocalStack placeholder role
                .bufferingHints(h -> h.sizeInMBs(1).intervalInSeconds(1)) // Configure buffering
                .build();

        CreateDeliveryStreamRequest request = CreateDeliveryStreamRequest.builder()
                .deliveryStreamName(STREAM_NAME)
                .deliveryStreamType(DeliveryStreamType.DIRECT_PUT)
                .extendedS3DestinationConfiguration(s3DestinationConfiguration)
                .build();

        fireHoseClient().createDeliveryStream(request);
    }

    private static void createS3Bucket(S3Client s3Client) {
        CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                .bucket(FIREHOSE_BUCKET)
                .build();
        s3Client.createBucket(createBucketRequest);
    }

    public static FirehoseClient fireHoseClient() {
        return FirehoseClient
                .builder()
                .endpointOverride(getAWSEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(getAWSAccessKey(), getAWSSecretKey())))
                .region(Region.of(getAWSRegion()))
                .build();
    }

    public static S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(getAWSEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(getAWSAccessKey(), getAWSSecretKey())))
                .region(Region.of(getAWSRegion()))
                .build();
    }

    @Override
    public void stop() {
        try {
            if (localStackContainer != null && localStackContainer.isRunning()) {
                localStackContainer.stop();
            }
        }
        catch (Exception e) {
            System.err.println("Failed to stop LocalStack container: " + e.getMessage());
        }
    }

    public static String getAWSRegion() {
        return localStackContainer.getRegion();
    }

    public static URI getAWSEndpoint() {
        return localStackContainer.getEndpointOverride(LocalStackContainer.Service.FIREHOSE);
    }

    public static String getAWSAccessKey() {
        return localStackContainer.getAccessKey();
    }

    public static String getAWSSecretKey() {
        return localStackContainer.getSecretKey();
    }
}
