package com.gojek.daggers.postProcessors.external.deprecated;

import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import com.gojek.de.stencil.StencilClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AshikoStreamDecoratorFactory {
    private static List<StreamDecorator> getAllDecorators(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity, int outputProtoSize) {
        return Arrays.asList(
                new EsStreamDecoratorDeprecated(configuration, stencilClient, asyncIOCapacity, fieldIndex),
                new TimestampDecorator(configuration, fieldIndex)
        );
    }

    public static StreamDecorator getStreamDecorator(Map<String, String> configuration, Integer fieldIndex, StencilClient stencilClient, Integer asyncIOCapacity, int outputProtoSize) {
        return getAllDecorators(configuration, fieldIndex, stencilClient, asyncIOCapacity, outputProtoSize)
                .stream()
                .filter(StreamDecorator::canDecorate)
                .findFirst()
                .orElse(new InputDecoratorDeprecated(configuration, fieldIndex, outputProtoSize));
    }
}



