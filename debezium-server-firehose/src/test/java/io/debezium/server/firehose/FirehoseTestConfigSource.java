/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.firehose;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;

import io.debezium.server.TestConfigSource;

public class FirehoseTestConfigSource extends TestConfigSource {

    public FirehoseTestConfigSource() {
        Map<String, String> firehoseTest = new HashMap<>();
        firehoseTest.put("debezium.sink.type", "firehose");
        firehoseTest.put("debezium.source.connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        firehoseTest.put("debezium.source." + StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, OFFSET_STORE_PATH.toAbsolutePath().toString());
        firehoseTest.put("debezium.source.offset.flush.interval.ms", "0");
        firehoseTest.put("debezium.source.topic.prefix", "testc");
        firehoseTest.put("debezium.source.schema.include.list", "inventory");
        firehoseTest.put("debezium.source.table.include.list", "inventory.customers");

        config = firehoseTest;
    }

    @Override
    public int getOrdinal() {
        return super.getOrdinal() + 1;
    }

    public static int waitForSeconds() {
        return 30;
    }

}
