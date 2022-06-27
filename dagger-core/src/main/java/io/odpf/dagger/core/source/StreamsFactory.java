package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.metrics.type.statsd.SerializedStatsDClientSupplier;
import io.odpf.dagger.core.source.config.StreamConfig;

import java.util.ArrayList;
import java.util.List;

public class StreamsFactory {
    public static List<Stream> getStreams(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, SerializedStatsDClientSupplier statsDClientSupplier) {
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        ArrayList<Stream> streams = new ArrayList<>();

        for (StreamConfig streamConfig : streamConfigs) {
            Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator, statsDClientSupplier);
            streams.add(builder.build());
        }
        return streams;
    }
}
