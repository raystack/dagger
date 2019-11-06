package com.gojek.daggers.postProcessors.external.deprecated;

import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;

import java.util.Map;

public class EsStreamDecoratorDeprecated implements AsyncDecorator {
    private Map<String, String> configuration;
    private StencilClient stencilClient;
    private Integer asyncIOCapacity;
    private Integer fieldIndex;

    public EsStreamDecoratorDeprecated(Map<String, String> configuration, StencilClient stencilClient, Integer asyncIOCapacity, Integer fieldIndex) {
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        this.asyncIOCapacity = asyncIOCapacity;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public Boolean canDecorate() {
        String source = configuration.get("source");
        return source.equals("es");
    }

    @Override
    public Integer getAsyncIOCapacity() {
        return asyncIOCapacity;
    }

    @Override
    public AsyncFunction getAsyncFunction() {
        return new ESAsyncConnectorDeprecated(fieldIndex, configuration, stencilClient);
    }

    @Override
    public Integer getStreamTimeout() {
        return Integer.valueOf(configuration.get("stream_timeout"));
    }

}
