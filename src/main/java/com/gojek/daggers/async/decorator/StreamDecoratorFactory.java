package com.gojek.daggers.async.decorator;

import com.gojek.daggers.async.decorator.async.EsStreamDecorator;
import com.gojek.daggers.async.decorator.map.InputDecorator;
import com.gojek.daggers.async.decorator.map.TimestampDecorator;
import com.gojek.de.stencil.StencilClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StreamDecoratorFactory {
    private static List<StreamDecorator> getAllDecorators(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity, int outputProtoSize) {
        return Arrays.asList(
                new EsStreamDecorator(configuration, stencilClient, asyncIOCapacity, fieldIndex),
                new TimestampDecorator(configuration, fieldIndex)
        );
    }

    public static StreamDecorator getStreamDecorator(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity, int outputProtoSize) {
        return getAllDecorators(configuration, fieldIndex, stencilClient, asyncIOCapacity, outputProtoSize)
                .stream()
                .filter(StreamDecorator::canDecorate)
                .findFirst()
                .orElse(new InputDecorator(configuration, fieldIndex, outputProtoSize));
    }
}



