package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.metrics.type.statsd.SerializedStatsDClientSupplier;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.deserializer.DaggerDeserializerFactory;
import io.odpf.dagger.core.source.config.StreamConfig;
import lombok.Getter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class Stream implements Serializable {
    @Getter
    private final DaggerSource<Row> daggerSource;
    @Getter
    private final String streamName;

    Stream(DaggerSource<Row> daggerSource, String streamName) {
        this.daggerSource = daggerSource;
        this.streamName = streamName;
    }

    public DataStream<Row> registerSource(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy) {
        return daggerSource.register(executionEnvironment, watermarkStrategy);
    }

    public static class Builder {
        private final StreamConfig streamConfig;
        private final Configuration configuration;
        private final StencilClientOrchestrator stencilClientOrchestrator;
        private final SerializedStatsDClientSupplier statsDClientSupplier;

        public Builder(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, SerializedStatsDClientSupplier statsDClientSupplier) {
            this.streamConfig = streamConfig;
            this.configuration = configuration;
            this.stencilClientOrchestrator = stencilClientOrchestrator;
            this.statsDClientSupplier = statsDClientSupplier;
        }

        public Stream build() {
            DaggerDeserializer<Row> daggerDeserializer = DaggerDeserializerFactory.create(streamConfig, configuration, stencilClientOrchestrator);
            DaggerSource<Row> daggerSource = DaggerSourceFactory.create(streamConfig, configuration, daggerDeserializer, statsDClientSupplier);
            return new Stream(daggerSource, streamConfig.getSchemaTable());
        }
    }
}
