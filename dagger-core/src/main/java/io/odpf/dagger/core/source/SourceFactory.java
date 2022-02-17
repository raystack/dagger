package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.source.kafka.KafkaSourceFactory;
import io.odpf.dagger.core.source.parquet.ParquetFileSourceFactory;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class SourceFactory {

    public static Source[] getSource(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        return Arrays.stream(streamConfig.getSourceDetails())
                .map(sourceDetails -> create(sourceDetails, streamConfig, configuration, deserializer))
                .toArray(Source[]::new);
    }

    private static Source create(SourceDetails sourceDetails, StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        SourceType sourceType = sourceDetails.getSourceType();
        SourceName sourceName = sourceDetails.getSourceName();
        switch (sourceName) {
            case PARQUET:
                return ParquetFileSourceFactory.getFileSource(sourceType, streamConfig, configuration, deserializer);
            case KAFKA:
            default:
                return KafkaSourceFactory.getSource(sourceType, streamConfig, configuration, (KafkaRecordDeserializationSchema<Row>) deserializer);
        }
    }
}