package io.odpf.dagger.core.deserializer;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DaggerDeserializer;
import io.odpf.dagger.core.exception.DaggerConfigurationException;
import io.odpf.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import io.odpf.dagger.core.metrics.reporters.statsd.StatsDErrorReporter;
import io.odpf.dagger.core.source.config.StreamConfig;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DaggerDeserializerFactory {
    public static DaggerDeserializer<Row> create(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator, SerializedStatsDReporterSupplier statsDReporterSupplier) {
        return getDaggerDeserializerProviders(streamConfig, configuration, stencilClientOrchestrator)
                .stream()
                .filter(DaggerDeserializerProvider::canProvide)
                .findFirst()
                .orElseThrow(() -> {
                    StatsDErrorReporter statsDErrorReporter = new StatsDErrorReporter(statsDReporterSupplier);
                    DaggerConfigurationException ex = new DaggerConfigurationException("No suitable deserializer could be constructed for the given stream configuration.");
                    statsDErrorReporter.reportFatalException(ex);
                    return ex;
                })
                .getDaggerDeserializer();
    }

    private static List<DaggerDeserializerProvider<Row>> getDaggerDeserializerProviders(StreamConfig streamConfig, Configuration configuration, StencilClientOrchestrator stencilClientOrchestrator) {
        return Stream.of(
                        new JsonDeserializerProvider(streamConfig),
                        new ProtoDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator),
                        new SimpleGroupDeserializerProvider(streamConfig, configuration, stencilClientOrchestrator))
                .collect(Collectors.toList());
    }
}
