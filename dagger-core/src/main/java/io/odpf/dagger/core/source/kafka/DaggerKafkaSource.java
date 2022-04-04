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
    private final KafkaDeserializationSchema deserializationSchema;
    private final Configuration configuration;

    public DaggerKafkaSource(StreamConfig streamConfig, KafkaDeserializationSchema deserializationSchema, Configuration configuration) {
        this.streamConfig = streamConfig;
        this.deserializationSchema = deserializationSchema;
        this.configuration = configuration;
    }

    @Override
    public boolean canHandle() {
        return SourceName.valueOf(streamConfig.getSourceType()).equals(SourceName.KAFKA_SOURCE);
    }

    @Override
    public DataStreamSource register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy, String streamName) {
        KafkaSource kafkaSource = getKafkaSource();
        return executionEnvironment.fromSource(kafkaSource, watermarkStrategy, streamName);
    }

    KafkaSource getKafkaSource() {
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamConfig.getTopicPattern())
                .setStartingOffsets(streamConfig.getStartingOffset())
                .setProperties(streamConfig.getKafkaProps(configuration))
                .setDeserializer(KafkaRecordDeserializationSchema.of(deserializationSchema))
                .build();
    }
}
