package com.gojek.daggers.longbow;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

public class LongBowReader extends RichAsyncFunction<Row, Row> {

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {

    }
}
