package io.odpf.dagger.core.source;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;

import java.util.ArrayList;
import java.util.List;

public class StreamsFactory {
    public static List<StreamType> getStreamTypes(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        StreamConfig[] streamConfigs = StreamConfig.parse(configuration);
        ArrayList<StreamType> streamTypes = new ArrayList<>();

        for (StreamConfig streamConfig : streamConfigs) {
            StreamType.Builder builder = new StreamType.Builder(streamConfig, configuration, stencilClientOrchestrator);
            streamTypes.add(builder.build());
        }
        return streamTypes;
    }
}
