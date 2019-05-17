package com.gojek.daggers.postprocessor;

import javafx.util.Pair;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public interface PostProcessor {

    Pair<DataStream<Row>, String[]> process(DataStream<Row> stream);
}
