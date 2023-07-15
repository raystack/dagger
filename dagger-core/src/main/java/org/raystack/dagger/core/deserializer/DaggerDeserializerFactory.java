package org.raystack.dagger.core.deserializer;

import org.raystack.dagger.common.configuration.Configuration;
import org.raystack.dagger.common.core.StencilClientOrchestrator;
import org.raystack.dagger.common.serde.DaggerDeserializer;
import org.raystack.dagger.core.exception.DaggerConfigurationException;
import org.raystack.dagger.core.metrics.reporters.statsd.SerializedStatsDReporterSupplier;
import org.raystack.dagger.core.metrics.reporters.statsd.StatsDErrorReporter;
import org.raystack.dagger.core.source.config.StreamConfig;
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
