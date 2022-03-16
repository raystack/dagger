package io.odpf.dagger.core.source.kafka;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.source.DaggerSource;
import io.odpf.dagger.core.source.SourceName;
import io.odpf.dagger.core.source.StreamConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

public class DaggerKafkaSource implements DaggerSource {
    private StreamConfig streamConfig;
    private KafkaSource kafkaSource;

    public DaggerKafkaSource(StreamConfig streamConfig, KafkaDeserializationSchema deserializationSchema, Configuration configuration) {
        this.streamConfig = streamConfig;
        this.kafkaSource = getKafkaSource(streamConfig, deserializationSchema, configuration);
    }

    @Override
    public boolean canHandle() {
        return SourceName.valueOf(streamConfig.getSourceType()).equals(SourceName.KAFKA_SOURCE);
    }

    @Override
    public DataStreamSource register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy, String streamName) {
        return executionEnvironment.fromSource(kafkaSource, watermarkStrategy, streamName);
    }

    KafkaSource getKafkaSource(StreamConfig config, KafkaDeserializationSchema deserializationSchema, Configuration configuration) {
        return KafkaSource.<Row>builder()
                .setTopicPattern(config.getTopicPattern())
                .setStartingOffsets(config.getStartingOffset())
                .setProperties(config.getKafkaProps(configuration))
                .setDeserializer(KafkaRecordDeserializationSchema.of(deserializationSchema))
                .build();
    }
}
