package com.gojek.daggers.decorator;

import com.gojek.daggers.builder.ResponseBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.Map;

public class InputDecorator implements StreamDecorator {
    private Map<String, String> configuration;
    private Integer fieldIndex;
    private int outputProtoSize;

    public InputDecorator(Map<String, String> configuration, Integer fieldIndex, int outputProtoSize) {
        this.configuration = configuration;
        this.fieldIndex = fieldIndex;
        this.outputProtoSize = outputProtoSize;
    }

    @Override
    public Boolean canDecorate() {
        String source = configuration.get("source");
        return source.equals("input");
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        return inputStream.map(this::enrichInput);
    }

    private Row enrichInput(Row input) {
        ResponseBuilder responseBuilder = new ResponseBuilder(outputProtoSize);
        responseBuilder.with(fieldIndex, input);
        return responseBuilder.build();
    }
}
