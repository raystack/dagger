package io.odpf.dagger.core.streamtype;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.source.StreamConfig;
import lombok.Getter;
import org.apache.flink.api.connector.source.Source;

public class StreamType<T> {
    @Getter
    private final Source source;
    @Getter
    private final String streamName;
    @Getter
    private final DataTypes inputDataType;
    @Getter
    private final DaggerDeserializer<T> deserializer;

    public StreamType(Source source, String streamName, DataTypes inputDataType, DaggerDeserializer<T> deserializer) {
        this.source = source;
        this.streamName = streamName;
        this.inputDataType = inputDataType;
        this.deserializer = deserializer;
    }

    public static abstract class Builder<T> {
        protected final StreamConfig streamConfig;
        protected final Configuration configuration;
        protected final StencilClientOrchestrator stencilClientOrchestrator;

        public Builder(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
            this.streamConfig = streamConfig;
            this.configuration = configuration;
            this.stencilClientOrchestrator = stencilClientOrchestrator;
        }

        public Builder(StreamConfig streamConfig, Configuration configuration) {
            this.streamConfig = streamConfig;
            this.configuration = configuration;
            this.stencilClientOrchestrator = null;
        }

        abstract StreamType<T> build();

        abstract boolean canBuild();
    }
}