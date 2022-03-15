package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.parquet.deserialization.SimpleGroupDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.source.kafka.KafkaSourceFactory;
import io.odpf.dagger.core.source.parquet.ParquetFileSource;
import io.odpf.dagger.core.source.parquet.ParquetFileSourceFactory;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.types.Row;

public class SourceFactory {

    public static Source create(SourceDetails sourceDetails,
                                StreamConfig streamConfig,
                                Configuration configuration,
                                DaggerDeserializer<Row> deserializer) {
        SourceType sourceType = sourceDetails.getSourceType();
        SourceName sourceName = sourceDetails.getSourceName();
        switch (sourceName) {
            case PARQUET:{
                SimpleGroupDeserializer simpleGroupDeserializer = (SimpleGroupDeserializer) deserializer;
                ParquetFileSource parquetFileSource = ParquetFileSourceFactory.getFileSource(sourceType,
                        streamConfig,
                        configuration,
                        simpleGroupDeserializer);
                return parquetFileSource.getFileSource();
            }
            case KAFKA: {
                KafkaDeserializationSchema<Row> deserializationSchema = (KafkaDeserializationSchema<Row>) deserializer;
                KafkaRecordDeserializationSchema<Row> recordDeserializationSchema = KafkaRecordDeserializationSchema.of(deserializationSchema);
                return KafkaSourceFactory.getSource(sourceType,
                        streamConfig,
                        configuration,
                        recordDeserializationSchema);
            }
            default: {
                String message = String.format("Invalid stream configuration: No suitable Flink DataSource could be " +
                        "constructed for source %s", sourceName.name());
                throw new DaggerConfigurationException(message);
            }
        }
    }
}