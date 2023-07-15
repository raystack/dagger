package org.raystack.dagger.core.processors.transformers;

import org.raystack.dagger.common.core.DaggerContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Row;

import org.raystack.dagger.common.core.StreamInfo;
import org.raystack.dagger.common.core.Transformer;

import java.util.Map;

public class MockTransformer implements Transformer, MapFunction<Row, Row> {
    public MockTransformer(Map<String, String> transformationArguments, String[] columnNames, DaggerContext daggerContext) {
    }

    @Override
    public StreamInfo transform(StreamInfo inputStreamInfo) {
        DataStream<Row> inputStream = inputStreamInfo.getDataStream();
        SingleOutputStreamOperator<Row> outputStream = inputStream.map(this);
        return new StreamInfo(outputStream, inputStreamInfo.getColumnNames());
    }

    @Override
    public Row map(Row value) throws Exception {
        return value;
    }
}
