package com.gojek.daggers.async.decorator;

import com.gojek.daggers.async.builder.ResponseBuilder;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Map;

public class TimestampDecorator implements MapDecorator {
    private Map<String, String> configuration;
    private Integer fieldIndex;

    TimestampDecorator(Map<String, String> configuration, Integer fieldIndex) {
        this.configuration = configuration;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public Boolean canDecorate() {
        String source = configuration.get("source");
        return source.equals("timestamp");
    }

    @Override
    public Row map(Row row) {
        ResponseBuilder responseBuilder = new ResponseBuilder(row);
        long timeInSeconds = (System.currentTimeMillis() + 10000) / 1000;
        Timestamp timestamp = new Timestamp(timeInSeconds * 1000);
        responseBuilder.with(fieldIndex, timestamp);
        return responseBuilder.build();
    }
}
