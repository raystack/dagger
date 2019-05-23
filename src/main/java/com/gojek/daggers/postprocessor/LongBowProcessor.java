package com.gojek.daggers.postprocessor;

import com.gojek.daggers.StreamInfo;
import com.gojek.daggers.longbow.LongBowReader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class LongBowProcessor implements PostProcessor {

    private HashMap<String, Integer> columnIndexMap;
    private Configuration configuration;

    public LongBowProcessor(Configuration configuration, String[] columnNames) {
        this.configuration = configuration;
        this.columnIndexMap = new HashMap<>();
        for (int i = 0; i < columnNames.length; i++) {
            this.columnIndexMap.put(columnNames[i], i);
        }
    }


    @Override
    public StreamInfo process(StreamInfo streamInfo) {
//        DataStream<Row> writeStream = AsyncDataStream.orderedWait(streamInfo.getDataStream(), (AsyncFunction) new LongBowWriter(), 5000, TimeUnit.MILLISECONDS, 40);
        DataStream<Row> readStream = AsyncDataStream.orderedWait(streamInfo.getDataStream(), (AsyncFunction) new LongBowReader(configuration, columnIndexMap), 5000, TimeUnit.MILLISECONDS, 40);
        return new StreamInfo(readStream, new String[1]);
    }
}
