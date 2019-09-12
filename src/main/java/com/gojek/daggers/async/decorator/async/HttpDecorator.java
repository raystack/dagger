package com.gojek.daggers.async.decorator.async;

import com.gojek.daggers.async.connector.HttpAsyncConnector;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;

import java.util.Map;

import static com.gojek.daggers.Constants.EXTERNAL_SOURCE_STREAM_TIMEOUT;

public class HttpDecorator implements AsyncDecorator {
    private HttpExternalSourceConfig httpExternalSourceConfig;
    private StencilClient stencilClient;
    private Integer asyncIOCapacity;
    private String type;
    private String[] columnNames;
    private String outputProto;

    public HttpDecorator(HttpExternalSourceConfig httpExternalSourceConfig, StencilClient stencilClient, Integer asyncIOCapacity,
                         String type, String[] columnNames, String outputProto) {
        this.httpExternalSourceConfig = httpExternalSourceConfig;
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
        return new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient, outputProto);
    }

    @Override
    public Integer getStreamTimeout() {
        return Integer.valueOf(httpExternalSourceConfig.getStreamTimeout());
    }
}
