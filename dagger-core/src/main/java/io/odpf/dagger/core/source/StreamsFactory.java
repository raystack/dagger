package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;

import java.util.ArrayList;
import java.util.List;

public class StreamsFactory {
    public static List<Stream> getStreams(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        ArrayList<Stream> streams = new ArrayList<>();

        for (StreamConfig streamConfig : streamConfigs) {
            Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator);
            streams.add(builder.build());
        }
        return streams;
    }
}
