package org.raystack.dagger.core.source.kafka;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.serde.DaggerDeserializer;
import org.raystack.dagger.core.source.DaggerSource;
import org.raystack.dagger.core.source.config.StreamConfig;
import org.raystack.dagger.core.source.config.models.SourceDetails;
import org.raystack.dagger.core.source.config.models.SourceName;
import org.raystack.dagger.core.source.config.models.SourceType;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

public class KafkaDaggerSource implements DaggerSource<Row> {
    private final DaggerDeserializer<Row> deserializer;
    private final StreamConfig streamConfig;
    private final Configuration configuration;
    private static final SourceName SUPPORTED_SOURCE_NAME = SourceName.KAFKA_SOURCE;
    private static final SourceType SUPPORTED_SOURCE_TYPE = SourceType.UNBOUNDED;

    public KafkaDaggerSource(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        this.streamConfig = streamConfig;
        this.configuration = configuration;
        this.deserializer = deserializer;
    }

    KafkaSource<Row> buildSource() {
        KafkaRecordDeserializationSchema<Row> kafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema
                .of((KafkaDeserializationSchema<Row>) deserializer);
        return KafkaSource.<Row>builder()
                .setTopicPattern(streamConfig.getTopicPattern())
                .setStartingOffsets(streamConfig.getStartingOffset())
                .setProperties(streamConfig.getKafkaProps(configuration))
                .setDeserializer(kafkaRecordDeserializationSchema)
                .build();
    }

    @Override
    public DataStream<Row> register(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy) {
        return executionEnvironment.fromSource(buildSource(), watermarkStrategy, streamConfig.getSchemaTable());
    }

    @Override
    public boolean canBuild() {
        SourceDetails[] sourceDetailsArray = streamConfig.getSourceDetails();
        if (sourceDetailsArray.length != 1) {
            return false;
        } else {
            SourceName sourceName = sourceDetailsArray[0].getSourceName();
            SourceType sourceType = sourceDetailsArray[0].getSourceType();
            return sourceName.equals(SUPPORTED_SOURCE_NAME) && sourceType.equals(SUPPORTED_SOURCE_TYPE)
                    && deserializer instanceof KafkaDeserializationSchema;
        }
    }
}
