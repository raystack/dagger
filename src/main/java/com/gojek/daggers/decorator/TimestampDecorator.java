package com.gojek.daggers.decorator;

import com.gojek.daggers.builder.ResponseBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Map;

public class TimestampDecorator implements StreamDecorator {
    private Map<String, String> configuration;
    private Integer fieldIndex;

    public TimestampDecorator(Map<String, String> configuration, Integer fieldIndex) {
        this.configuration = configuration;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public Boolean canDecorate() {
        String source = configuration.get("source");
        return source.equals("timestamp");
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        return inputStream.map(this::enrich);
    }

    private Row enrich(Row row) {
        ResponseBuilder responseBuilder = new ResponseBuilder(row);
        long timeInSeconds = (System.currentTimeMillis() + 10000) / 1000;
        Timestamp timestamp = new Timestamp(timeInSeconds * 1000);
        responseBuilder.with(fieldIndex, timestamp);
        return responseBuilder.build();
    }
}
