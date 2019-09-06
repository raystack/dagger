package com.gojek.daggers.async.decorator.async;

import com.gojek.daggers.async.connector.HttpAsyncConnector;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;

import java.util.Map;

public class HttpDecorator implements AsyncDecorator {
    private Map<String, Object> configuration;
    private StencilClient stencilClient;
    private Integer asyncIOCapacity;
    private String type;
    private String[] columnNames;
    private String outputProto;

    public HttpDecorator(Map<String, Object> configuration, StencilClient stencilClient, Integer asyncIOCapacity,
                         String type, String[] columnNames, String outputProto) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        this.asyncIOCapacity = asyncIOCapacity;
        this.type = type;
        this.columnNames = columnNames;
        this.outputProto = outputProto;
    }

    @Override
    public Boolean canDecorate() {
        return type.equals("http");
    }

    @Override
    public Integer getAsyncIOCapacity() {
        return asyncIOCapacity;
    }

    @Override
    public AsyncFunction getAsyncFunction() {
        return new HttpAsyncConnector(columnNames, configuration, stencilClient, outputProto);
    }

    @Override
    public Integer getStreamTimeout() {
        return Integer.valueOf((String) configuration.get("stream_timeout"));
    }
}
