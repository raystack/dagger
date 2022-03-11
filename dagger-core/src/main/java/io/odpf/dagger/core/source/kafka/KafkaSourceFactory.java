package io.odpf.dagger.core.source.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.SourceType;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import static io.odpf.dagger.core.source.SourceType.BOUNDED;

public class KafkaSourceFactory {
    public static KafkaSource<Row> getSource(SourceType sourceType, StreamConfig streamConfig, Configuration configuration, KafkaRecordDeserializationSchema<Row> deserializer) {
        if (sourceType == BOUNDED) {
            throw new IllegalConfigurationException("Running KafkaSource in BOUNDED mode is not supported yet");
        }
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamConfig.getTopicPattern())
                .setStartingOffsets(streamConfig.getStartingOffset())
                .setProperties(streamConfig.getKafkaProps(configuration))
                .setDeserializer(deserializer)
                .build();
    }
}