/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.firehose;

import static io.debezium.server.firehose.FirehoseResourceLifecycleManager.FIREHOSE_BUCKET;
import static io.debezium.server.firehose.FirehoseResourceLifecycleManager.s3Client;

import java.time.Duration;

import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import io.debezium.server.DebeziumServer;
import io.debezium.server.events.ConnectorCompletedEvent;
import io.debezium.testing.testcontainers.PostgresTestResourceLifecycleManager;
import io.debezium.util.Testing;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;

@QuarkusTest
@QuarkusTestResource(PostgresTestResourceLifecycleManager.class)
@QuarkusTestResource(FirehoseResourceLifecycleManager.class)
public class FirehoseIT {

    {
        Testing.Files.delete(FirehoseTestConfigSource.OFFSET_STORE_PATH);
        Testing.Files.createTestingFile(FirehoseTestConfigSource.OFFSET_STORE_PATH);
    }

    @Inject
    DebeziumServer server;

    void connectorCompleted(@Observes ConnectorCompletedEvent event) throws Exception {
        if (!event.isSuccess()) {
            throw (Exception) event.getError().get();
        }
    }

    @Test
    public void testFirehose() throws Exception {
        Testing.Print.enable();
        Awaitility.await().atMost(Duration.ofSeconds(FirehoseTestConfigSource.waitForSeconds())).until(() -> {
            final ListObjectsResponse objectList = s3Client().listObjects(ListObjectsRequest.builder().bucket(FIREHOSE_BUCKET).build());
            return !objectList.contents().isEmpty();
        });
    }
}
