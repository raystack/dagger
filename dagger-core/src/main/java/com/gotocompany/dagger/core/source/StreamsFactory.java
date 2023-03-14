package com.gotocompany.dagger.core.source;

import com.gotocompany.dagger.common.configuration.Configuration;
import com.gotocompany.dagger.common.core.StencilClientOrchestrator;
import com.gotocompany.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import com.gotocompany.dagger.core.source.config.StreamConfig;

import java.util.ArrayList;
import java.util.List;

public class StreamsFactory {
    public static List<Stream> getStreams(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, SerializedStatsDReporterSupplier statsDReporterSupplier) {
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        ArrayList<Stream> streams = new ArrayList<>();

        for (StreamConfig streamConfig : streamConfigs) {
            Stream.Builder builder = new Stream.Builder(streamConfig, configuration, stencilClientOrchestrator, statsDReporterSupplier);
            streams.add(builder.build());
        }
        return streams;
    }
}
