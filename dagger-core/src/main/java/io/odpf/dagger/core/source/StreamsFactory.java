package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.source.kafka.builder.JsonDataStreamBuilder;
import io.odpf.dagger.core.source.kafka.builder.ProtoDataStreamBuilder;
import io.odpf.dagger.core.source.kafka.builder.StreamBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamsFactory {

    public static List<Stream> getStreams(Configuration configuration,
                                          StencilClientOrchestrator stencilClientOrchestrator,
                                          MetricsTelemetryExporter telemetryExporter) {
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        ArrayList<Stream> streams = new ArrayList<>();

        for (StreamConfig streamConfig : streamConfigs) {
            List<StreamBuilder> dataStreams = Arrays
                    .asList(new JsonDataStreamBuilder(streamConfig, configuration),
                            new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration));
            StreamBuilder streamBuilder = dataStreams.stream()
                    .filter(dataStream -> dataStream.canBuild())
                    .findFirst()
                    .orElse(new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration));

            streamBuilder.notifySubscriber(telemetryExporter);

            Stream stream = streamBuilder
                    .build();
            streams.add(stream);
        }
        return streams;
    }
}

