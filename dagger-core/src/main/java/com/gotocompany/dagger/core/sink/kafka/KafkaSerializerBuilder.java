package com.gotocompany.dagger.core.sink.kafka;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

public interface KafkaSerializerBuilder {
    KafkaRecordSerializationSchema build();
}
