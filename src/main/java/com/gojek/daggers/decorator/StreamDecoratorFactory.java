package com.gojek.daggers.decorator;

import com.gojek.de.stencil.StencilClient;
import com.timgroup.statsd.StatsDClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StreamDecoratorFactory {
    private static List<StreamDecorator> getAsyncTypes(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity) {
        return Arrays.asList(
                new EsStreamDecorator(configuration, stencilClient, asyncIOCapacity, fieldIndex),
                new TimestampDecorator(configuration, fieldIndex)
        );
    }

    public static StreamDecorator getStreamDecorator(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity, int outputProtoSize) {
        return getAsyncTypes(configuration, fieldIndex, stencilClient, asyncIOCapacity)
                .stream()
                .filter(StreamDecorator::canDecorate)
                .findFirst()
                .orElse(new InputDecorator(configuration, fieldIndex, outputProtoSize));
    }
}



