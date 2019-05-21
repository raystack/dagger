package com.gojek.daggers.postprocessor;

import com.gojek.de.stencil.StencilClient;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

import static com.gojek.daggers.Constants.ASYNC_IO_ENABLED_DEFAULT;
import static com.gojek.daggers.Constants.ASYNC_IO_ENABLED_KEY;

public class PostProcessorFactory {
    public static Optional<PostProcessor> getPostProcessor(Configuration configuration, StencilClient stencilClient, String[] columnNames) {
        if (configuration.getBoolean(ASYNC_IO_ENABLED_KEY, ASYNC_IO_ENABLED_DEFAULT))
            return Optional.of(new AshikoProcessor(configuration, stencilClient));
        return Optional.empty();
    }
}
