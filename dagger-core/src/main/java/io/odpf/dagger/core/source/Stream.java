package io.odpf.dagger.core.source;

import io.odpf.dagger.common.serde.DataTypes;
import lombok.Getter;
import lombok.NonNull;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class Stream {
    @Getter
    @NonNull
    private final DaggerSource daggerSource;
    @Getter
    @NonNull
    private final String streamName;
    @Getter
    @NonNull
    private DataTypes inputDataType;

    public Stream(DaggerSource daggerSource, String streamName, DataTypes inputDataType) {
        this.daggerSource = daggerSource;
        this.streamName = streamName;
        this.inputDataType = inputDataType;
    }

    public DataStream<Row> registerSource(StreamExecutionEnvironment executionEnvironment, WatermarkStrategy<Row> watermarkStrategy, String stream) {
        return daggerSource.register(executionEnvironment, watermarkStrategy, stream);
    }
}

