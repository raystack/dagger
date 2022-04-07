package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.exception.InvalidDaggerSourceException;
import io.odpf.dagger.core.source.kafka.DaggerKafkaSource;
import io.odpf.dagger.core.source.kafka.DaggerOldKafkaSource;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.stream.Stream;

public class DaggerSourceFactory {

    public static DaggerSource createDaggerSource(StreamConfig streamConfig, KafkaDeserializationSchema deserializationSchema, Configuration configuration) {
        return Stream.of(new DaggerOldKafkaSource(streamConfig, deserializationSchema, configuration), new DaggerKafkaSource(streamConfig, deserializationSchema, configuration))
                .filter(DaggerSource::canHandle)
                .findFirst()
                .orElseThrow(() -> new InvalidDaggerSourceException(String.format("Dagger source: %s not supported", streamConfig.getSourceType())));
    }
}
