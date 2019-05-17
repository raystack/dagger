package com.gojek.daggers.postprocessor;

import com.gojek.daggers.longbow.LongBowReader;
import com.gojek.daggers.longbow.LongBowWriter;
import javafx.util.Pair;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class LongBowProcessor implements PostProcessor {

    @Override
    public Pair<DataStream<Row>, String[]> process(DataStream<Row> stream) {
        DataStream<Row> writeStream = AsyncDataStream.orderedWait(stream, (AsyncFunction) new LongBowWriter(), 5000, TimeUnit.MILLISECONDS, 40);
        DataStream<Row> readStream = AsyncDataStream.orderedWait(writeStream, (AsyncFunction) new LongBowReader(), 5000, TimeUnit.MILLISECONDS, 40);
        return new Pair<>(readStream, new String[1]);
    }
}
