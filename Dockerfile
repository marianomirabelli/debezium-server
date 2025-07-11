FROM debezium/server:3.0.0.Final
COPY debezium-server-dist/target/lib /debezium/lib/
COPY debezium-server-dist/target/debezium-server-dist-3.2.0-SNAPSHOT-runner.jar /debezium/debezium-server-dist-3.0.0.Final-runner.jar