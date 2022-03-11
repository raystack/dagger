package io.odpf.dagger.core.source;

import lombok.NonNull;
import org.apache.flink.api.connector.source.Source;

import io.odpf.dagger.common.serde.DataTypes;
import lombok.Getter;

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
    @Getter
    @NonNull
    private SourceDetails[] sourceDetails;

    public Stream(Source source, String streamName, DataTypes inputDataType) {
        this.source = source;
        this.streamName = streamName;
        this.inputDataType = inputDataType;
        this.sourceDetails = new SourceDetails[0];
    }

    public Stream(Source source, SourceDetails[] sourceDetails, String streamName, DataTypes inputDataType) {
        this.source = source;
        this.streamName = streamName;
        this.inputDataType = inputDataType;
        this.sourceDetails = sourceDetails;
    }
}