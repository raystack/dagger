package io.odpf.dagger.common.core;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * The class to hold the data stream and column names.
 */
public class StreamInfo {
    private DataStream<Row> dataStream;
    private String[] columnNames;

    /**
     * Instantiates a new Stream info.
     *
     * @param dataStream  the data stream
     * @param columnNames the column names
     */
    public StreamInfo(DataStream<Row> dataStream, String[] columnNames) {
        this.dataStream = dataStream;
        this.columnNames = columnNames;
    }

    /**
     * Gets data stream.
     *
     * @return the data stream
     */
    public DataStream<Row> getDataStream() {
        return dataStream;
    }

    /**
     * Get column names.
     *
     * @return list of column names
     */
    public String[] getColumnNames() {
        return columnNames;
    }
}
