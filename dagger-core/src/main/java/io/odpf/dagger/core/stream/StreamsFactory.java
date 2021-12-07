package io.odpf.dagger.core.stream;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.stream.builder.JsonDataStreamBuilder;
import io.odpf.dagger.core.stream.builder.ProtoDataStreamBuilder;
import io.odpf.dagger.core.stream.builder.StreamBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamsFactory {

    public static List<Stream> getStreams(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        ArrayList<Stream> streams = new ArrayList<>();

        for (StreamConfig streamConfig : streamConfigs) {
            List<StreamBuilder> dataStreams = Arrays
                    .asList(new JsonDataStreamBuilder(streamConfig, configuration),
                            new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration));
            Stream stream = dataStreams.stream()
                    .filter(dataStream -> dataStream.canBuild())
                    .findFirst()
                    .orElse(new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration))
                    .build();
            streams.add(stream);
        }
        return streams;
    }
}

