package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.exception.InvalidDaggerSourceException;
import io.odpf.dagger.core.source.flinkkafkaconsumer.FlinkKafkaConsumerDaggerSource;
import io.odpf.dagger.core.source.kafka.KafkaDaggerSource;
import io.odpf.dagger.core.source.parquet.ParquetDaggerSource;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DaggerSourceFactory {

    public static DaggerSource<Row> create(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        List<DaggerSource<Row>> daggerSources = getDaggerSources(streamConfig, configuration, deserializer);
        return daggerSources.stream()
                .filter(DaggerSource::canBuild)
                .findFirst()
                .orElseThrow(() -> {
                    String sourceDetails = Arrays.toString(streamConfig.getSourceDetails());
                    String message = String.format("No suitable DaggerSource can be created as per SOURCE_DETAILS config %s", sourceDetails);
                    return new InvalidDaggerSourceException(message);
                });
    }

    private static List<DaggerSource<Row>> getDaggerSources(StreamConfig streamConfig, Configuration configuration, DaggerDeserializer<Row> deserializer) {
        KafkaDaggerSource kafkaDaggerSource = new KafkaDaggerSource(streamConfig, configuration, deserializer);
        FlinkKafkaConsumerDaggerSource flinkKafkaConsumerDaggerSource = new FlinkKafkaConsumerDaggerSource(streamConfig, configuration, deserializer);
        ParquetDaggerSource parquetDaggerSource = new ParquetDaggerSource(streamConfig, configuration, deserializer);
        return Stream.of(kafkaDaggerSource, flinkKafkaConsumerDaggerSource, parquetDaggerSource)
                .collect(Collectors.toList());
    }
}
