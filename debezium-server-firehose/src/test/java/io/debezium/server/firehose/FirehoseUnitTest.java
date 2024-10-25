/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.firehose;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import jakarta.enterprise.inject.Instance;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.Header;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
public class FirehoseUnitTest {

    private FirehoseChangeConsumer firehoseChangeConsumer;
    private FirehoseClient spyClient;
    private AtomicInteger counter;
    private AtomicBoolean threwException;
    List<ChangeEvent<Object, Object>> changeEvents;
    RecordCommitter<ChangeEvent<Object, Object>> committer;

    @BeforeEach
    public void setup() {
        counter = new AtomicInteger(0);
        threwException = new AtomicBoolean(false);
        changeEvents = createChangeEvents(300, "key", "destination");
        committer = RecordCommitter();
        spyClient = spy(FirehoseClient.builder().region(Region.of("us-east-1"))
                .credentialsProvider(ProfileCredentialsProvider.create("default")).build());

        Instance<FirehoseClient> mockInstance = mock(Instance.class);
        when(mockInstance.isResolvable()).thenReturn(true);
        when(mockInstance.get()).thenReturn(spyClient);

        firehoseChangeConsumer = new FirehoseChangeConsumer();
        firehoseChangeConsumer.customClient = mockInstance;
    }

    @AfterEach
    public void tearDown() {
        reset(spyClient);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static List<ChangeEvent<Object, Object>> createChangeEvents(int size, String key, String destination) {
        List<ChangeEvent<Object, Object>> changeEvents = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            ChangeEvent<Object, Object> result = mock(ChangeEvent.class);
            when(result.key()).thenReturn(key);
            when(result.value()).thenReturn(Integer.toString(i));
            when(result.destination()).thenReturn(destination);
            Header header = mock(Header.class);
            when(header.getKey()).thenReturn(key);
            when(header.getValue()).thenReturn(Integer.toString(i));
            when(result.headers()).thenReturn(List.of(header));
            changeEvents.add(result);
        }
        return changeEvents;
    }

    @SuppressWarnings({"unchecked"})
    private static RecordCommitter<ChangeEvent<Object, Object>> RecordCommitter() {
        RecordCommitter<ChangeEvent<Object, Object>> result = mock(RecordCommitter.class);
        return result;
    }

    @Test
    @DisplayName("Continous sending of Firehose response containing error yields exception after 5 attempts")
    public void testValidResponseWithErrorCode() throws Exception {
        // Arrange
        doAnswer(invocation -> {
            PutRecordBatchRequest request = invocation.getArgument(0);
            List<Record> records = request.records();
            counter.incrementAndGet();
            List<PutRecordBatchResponseEntry> failedEntries = records.stream()
                    .map(record -> PutRecordBatchResponseEntry.builder().errorCode("ProvisionedThroughputExceededException")
                            .errorMessage("The request rate for the stream is too high").build())
                    .collect(Collectors.toList());
            return PutRecordBatchResponse.builder().requestResponses(failedEntries).failedPutCount(records.size()).build();
        }).when(spyClient).putRecordBatch(any(PutRecordBatchRequest.class));

        try {
            firehoseChangeConsumer.connect();
            firehoseChangeConsumer.handleBatch(changeEvents, RecordCommitter());
        } catch (Exception e) {
            threwException.getAndSet(true);
        }

        assertTrue(threwException.get());
        assertEquals(5, counter.get());
    }

    @Test
    @DisplayName("Only failed records are re-sent")
    public void testResendFailedRecords() throws Exception {

        AtomicBoolean firstCall = new AtomicBoolean(true);
        List<Record> failedRecordsFromFirstCall = new ArrayList<>();
        List<Record> recordsFromSecondCall = new ArrayList<>();

        doAnswer(invocation -> {
            List<PutRecordBatchResponseEntry> response = new ArrayList<>();
            PutRecordBatchRequest request = invocation.getArgument(0);
            List<Record> records = request.records();
            counter.incrementAndGet();

            if (firstCall.get()) {
                int failedEntries = 100;
                for (int i = 0; i < records.size(); i++) {
                    PutRecordBatchResponseEntry recordResult;
                    if (i < failedEntries) {
                        recordResult = PutRecordBatchResponseEntry.builder().errorCode("ProvisionedThroughputExceededException")
                                .errorMessage("The request rate for the stream is too high").build();

                        failedRecordsFromFirstCall.add(records.get(i));
                    } else {
                        recordResult = PutRecordBatchResponseEntry.builder().recordId("recordId").build();
                    }
                    response.add(recordResult);
                }
                firstCall.getAndSet(false);
                return PutRecordBatchResponse.builder().requestResponses(response).failedPutCount(response.size()).build();
            } else {
                for (Record record : records) {
                    recordsFromSecondCall.add(record);
                    PutRecordBatchResponseEntry recordResult = PutRecordBatchResponseEntry.builder().recordId("recordId").build();
                    response.add(recordResult);
                }
                return PutRecordBatchResponse.builder().failedPutCount(0).requestResponses(response).build();
            }
        }).when(spyClient).putRecordBatch(any(PutRecordBatchRequest.class));

        try {
            firehoseChangeConsumer.connect();
            firehoseChangeConsumer.handleBatch(changeEvents, committer);
        } catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(2, counter.get());
        assertEquals(recordsFromSecondCall.size(), failedRecordsFromFirstCall.size());
        for (int i = 0; i < recordsFromSecondCall.size(); i++) {
            assertEquals(failedRecordsFromFirstCall.get(i).data(), recordsFromSecondCall.get(i).data());
        }
    }

    @Test
    @DisplayName("Batch of 1000 records is correctly split into 2 batches of 500 records")
    public void testBatchSplitting() throws Exception {
        List<ChangeEvent<Object, Object>> changeEvents = createChangeEvents(1000, "key", "destination");

        AtomicInteger numBatches = new AtomicInteger(0);
        AtomicInteger numRecordsBatchOne = new AtomicInteger(0);
        AtomicInteger numRecordsBatchTwo = new AtomicInteger(0);
        AtomicBoolean firstBatch = new AtomicBoolean(true);

        doAnswer(invocation -> {
            List<PutRecordBatchResponseEntry> response = new ArrayList<>();
            PutRecordBatchRequest request = invocation.getArgument(0);
            List<Record> records = request.records();

            for (Record record : records) {
                if (firstBatch.get()) {
                    numRecordsBatchOne.incrementAndGet();
                } else {
                    numRecordsBatchTwo.incrementAndGet();
                }
                PutRecordBatchResponseEntry recordResult = PutRecordBatchResponseEntry.builder().recordId("recordId").build();
                response.add(recordResult);
            }
            numBatches.incrementAndGet();
            firstBatch.getAndSet(false);
            return PutRecordBatchResponse.builder().failedPutCount(0).requestResponses(response).build();
        }).when(spyClient).putRecordBatch(any(PutRecordBatchRequest.class));

        try {
            firehoseChangeConsumer.connect();
            firehoseChangeConsumer.handleBatch(changeEvents, committer);
        } catch (Exception e) {
            threwException.getAndSet(true);
        }

        // Assert
        assertFalse(threwException.get());
        assertEquals(2, numBatches.get());
        assertEquals(500, numRecordsBatchOne.get());
        assertEquals(500, numRecordsBatchTwo.get());
    }
}
