/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.firehose;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.server.BaseChangeConsumer;
import io.debezium.server.CustomConsumerBuilder;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.FirehoseClientBuilder;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.Record;

@Named("firehose")
@Dependent
public class FirehoseChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirehoseChangeConsumer.class);

    private static final String PROP_PREFIX = "debezium.sink.firehose.";
    private static final String PROP_REGION_NAME = PROP_PREFIX + "region";
    private static final String PROP_ENDPOINT_NAME = PROP_PREFIX + "endpoint";
    private static final String PROP_CREDENTIALS_PROFILE = PROP_PREFIX + "credentials.profile";
    private static final String PROP_STREAM_NAME = PROP_PREFIX + "stream.name";
    private static final String PROP_BATCH_SIZE = PROP_PREFIX + "batch.size";
    private static final String PROP_RETRIES_MAX_ATTEMPTS = PROP_PREFIX + "retries.max.attempts";
    private static final String PROP_RETRIES_BASE_DELAY = PROP_PREFIX + "retries.base.delay";
    private static final String PROP_RETRIES_MAX_DELAY = PROP_PREFIX + "retries.max.delay";

    private static final int DEFAULT_RETRY_COUNT = 5;
    private static final int DEFAULT_RETRY_BASE_DELAY = 2;
    private static final int DEFAULT_RETRY_MAX_DELAY = 20;
    private static final int DEFAULT_BATCH_SIZE = 500;
    private static final String DEFAULT_STREAM_NAME = "data-stream";

    private static final int MAX_BATCH_SIZE = 500;

    private static final byte LF = 10; // Line Feed

    private String region;
    private Optional<String> endpointOverride;
    private Optional<String> credentialsProfile;

    private Integer baseDelay;
    private Integer maxDelay;
    private Integer batchSize;

    private Integer maxRetries;

    private String streamName;

    private ObjectMapper mapper = null;

    @Inject
    @CustomConsumerBuilder
    Instance<ObjectMapper> customMapper;

    @ConfigProperty(name = PROP_PREFIX + "null.key", defaultValue = "default")
    String nullKey;

    private FirehoseClient client = null;

    @Inject
    @CustomConsumerBuilder
    Instance<FirehoseClient> customClient;

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        maxRetries = config.getOptionalValue(PROP_RETRIES_MAX_ATTEMPTS, Integer.class).orElse(DEFAULT_RETRY_COUNT);
        baseDelay = config.getOptionalValue(PROP_RETRIES_BASE_DELAY, Integer.class).orElse(DEFAULT_RETRY_BASE_DELAY);
        maxDelay = config.getOptionalValue(PROP_RETRIES_MAX_DELAY, Integer.class).orElse(DEFAULT_RETRY_MAX_DELAY);
        streamName = config.getOptionalValue(PROP_STREAM_NAME, String.class).orElse(DEFAULT_STREAM_NAME);
        batchSize = config.getOptionalValue(PROP_BATCH_SIZE, Integer.class).orElse(DEFAULT_BATCH_SIZE);
        region = config.getValue(PROP_REGION_NAME, String.class);
        endpointOverride = config.getOptionalValue(PROP_ENDPOINT_NAME, String.class);
        credentialsProfile = config.getOptionalValue(PROP_CREDENTIALS_PROFILE, String.class);

        mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        mapper.registerModule(new JavaTimeModule());

        if (batchSize <= 0) {
            throw new DebeziumException("Batch size must be greater than 0");
        }
        else if (batchSize > MAX_BATCH_SIZE) {
            throw new DebeziumException("Batch size must be less than or equal to MAX_BATCH_SIZE");
        }

        if (customClient.isResolvable()) {
            client = customClient.get();
            LOGGER.info("Obtained custom configured FirehoseClient '{}'", client);
            return;
        }

        final FirehoseClientBuilder builder = FirehoseClient.builder()
                .region(Region.of(region));

        endpointOverride.ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
        credentialsProfile.ifPresent(profile -> builder.credentialsProvider(ProfileCredentialsProvider.create(profile)));

        client = builder.build();
        LOGGER.info("Using default FireHoseClient '{}'", client);
    }

    @PreDestroy
    void close() {
        try {
            client.close();
        }
        catch (Exception e) {
            LOGGER.warn("Exception while closing FireHose client: {}", e);
        }
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        if (records.isEmpty()) {
            committer.markBatchFinished();
            return;
        }

        if (records.size() < batchSize) {
            buildAndSendRecords(records, committer);
        }
        else {
            int size = records.size();
            int i = 0;
            while (i < size) {
                int lastIndex = Math.min(i + batchSize, size);
                buildAndSendRecords(records.subList(i, lastIndex), committer); // lastIndex is non-inclusive
                i = lastIndex;
            }
        }

        committer.markBatchFinished();

    }

    private void buildAndSendRecords(List<ChangeEvent<Object, Object>> records, RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        List<Record> firehoseRecords = records.stream()
                .map(
                        t -> Record.builder()
                                .data(toSdkBytes(t))
                                .build())
                .collect(Collectors.toList());
        sendData(firehoseRecords);

        for (ChangeEvent<Object, Object> record : records) {
            committer.markProcessed(record);
        }
    }

    private SdkBytes toSdkBytes(ChangeEvent<Object, Object> event) {
        byte[] originalBytes = getBytes(toJson(event));
        byte[] modifiedBytes = new byte[originalBytes.length + 1];
        System.arraycopy(originalBytes, 0, modifiedBytes, 0, originalBytes.length);
        modifiedBytes[modifiedBytes.length - 1] = LF; // This adds an LF byte because Firehose requires it to save events as separated entries.
        return SdkBytes.fromByteArray(modifiedBytes);
    }

    private String toJson(ChangeEvent<Object, Object> event) {
        Map<String, Object> record = new HashMap<>();
        String key = Optional.ofNullable(getString(event.key())).orElse(nullKey);
        Object eventValue = Optional.ofNullable(event.value()).orElse("");
        record.put("key", key);
        record.put("data", eventValue);
        try {
            return mapper.writeValueAsString(record);
        }
        catch (JsonProcessingException e) {
            LOGGER.error("Failed to serialize sink registry to send by firehose", e);
            throw new DebeziumException("Failed to serialize sink registry to send by firehose");
        }
    }

    private void sendData(List<Record> originalRecords) throws InterruptedException {
        int attempts = 1;
        List<Record> recordsToSend = originalRecords;

        while (attempts <= maxRetries) {

            PutRecordBatchRequest batchRecord = PutRecordBatchRequest.builder()
                    .deliveryStreamName(streamName)
                    .records(recordsToSend)
                    .build();

            PutRecordBatchResponse response;

            try {
                response = client.putRecordBatch(batchRecord);

            }
            catch (FirehoseException ex) {
                LOGGER.error("{} records have not been ingested", ex);
                throw new DebeziumException("Failed to ingest records on firehose", ex);
            }

            if (response.failedPutCount() > 0) {
                List<Record> retryableRecords = new ArrayList<>();

                for (int i = 0; i < response.requestResponses().size(); i++) {
                    if (response.requestResponses().get(i).errorCode() != null && !response.requestResponses().get(i).errorCode().isEmpty()) {
                        retryableRecords.add(recordsToSend.get(i));
                    }
                }
                double backoff = Math.min(Math.pow(baseDelay, attempts), maxDelay);
                LOGGER.warn("Attempt number {} to retry failed processed events", attempts);
                Metronome.sleeper(Duration.ofSeconds((long) backoff), Clock.SYSTEM).pause();
                recordsToSend = retryableRecords;
                attempts++;

            }
            else {
                return;
            }
        }

        double recordsLost = recordsToSend.size();
        LOGGER.error("{} records have not been ingested", recordsLost);
        throw new DebeziumException("Exceeded maximum number of attempts to publish event");

    }
}
