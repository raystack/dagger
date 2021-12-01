package io.odpf.dagger.core.stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.core.stream.builder.JsonDataStreamBuilder;
import io.odpf.dagger.core.stream.builder.ProtoDataStreamBuilder;
import io.odpf.dagger.core.stream.builder.StreamBuilder;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.odpf.dagger.common.core.Constants.INPUT_STREAMS;

public class StreamsFactory {
    private static final Gson GSON = new GsonBuilder()
            .enableComplexMapKeySerialization()
            .setPrettyPrinting()
            .create();

    public static List<Stream> getStreams(Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        String jsonArrayString = configuration.getString(INPUT_STREAMS, "");
        JsonReader reader = new JsonReader(new StringReader(jsonArrayString));
        reader.setLenient(true);

        StreamConfig[] streamsConfigs = GSON.fromJson(jsonArrayString, StreamConfig[].class);

        ArrayList<Stream> streams = new ArrayList<>();

        for (StreamConfig streamConfig : streamsConfigs) {
            List<StreamBuilder> dataStreams = Arrays
                    .asList(new StreamBuilder[]{new JsonDataStreamBuilder(streamConfig, configuration),
                            new ProtoDataStreamBuilder(streamConfig, stencilClientOrchestrator, configuration)});
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

