package io.odpf.dagger.core.source.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

public class KafkaSourceFactory {
    public static KafkaSource<Row> getSource(SourceType sourceType, StreamConfig streamConfig, Configuration configuration, KafkaRecordDeserializationSchema<Row> deserializer) {
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamConfig.getTopicPattern())
                .setStartingOffsets(streamConfig.getStartingOffset())
                .setProperties(streamConfig.getKafkaProps(configuration))
                .setDeserializer(deserializer)
                .build();
    }
}
