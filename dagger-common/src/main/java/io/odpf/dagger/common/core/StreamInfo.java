package io.odpf.dagger.common.core;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public class StreamInfo {
    private DataStream<Row> dataStream;
    private String[] columnNames;

    public StreamInfo(DataStream<Row> dataStream, String[] columnNames) {
        this.dataStream = dataStream;
        this.columnNames = columnNames;
    }

    public DataStream<Row> getDataStream() {
        return dataStream;
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}
