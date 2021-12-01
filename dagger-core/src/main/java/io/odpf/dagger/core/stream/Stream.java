package io.odpf.dagger.core.stream;

import org.apache.flink.api.connector.source.Source;

import io.odpf.dagger.common.serde.DataTypes;

public class Stream {
    private final Source source;
    private final String streamName;
    private DataTypes inputDataType;

    public Stream(Source source, String streamName, DataTypes inputDataType) {
        this.source = source;
        this.streamName = streamName;
        this.inputDataType = inputDataType;
    }

    public String getStreamName() {
        return streamName;
    }

    public Source getSource() {
        return source;
    }
}
