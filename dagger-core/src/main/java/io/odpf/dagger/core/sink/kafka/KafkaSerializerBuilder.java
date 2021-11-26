package io.odpf.dagger.core.sink.kafka;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

// TODO :  A better name ?
public interface KafkaSerializerBuilder {
    KafkaRecordSerializationSchema build();
}
