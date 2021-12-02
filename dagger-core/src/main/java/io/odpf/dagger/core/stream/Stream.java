package io.odpf.dagger.core.stream;

import org.apache.flink.api.connector.source.Source;

import io.odpf.dagger.common.serde.DataTypes;
import lombok.Getter;
import lombok.NonNull;

public class Stream {
    @Getter
    @NonNull
    private final Source source;
    @Getter
    @NonNull
    private final String streamName;
    @Getter
    @NonNull
    private DataTypes inputDataType;

    public Stream(Source source, String streamName, DataTypes inputDataType) {
        this.source = source;
        this.streamName = streamName;
        this.inputDataType = inputDataType;
    }
}

