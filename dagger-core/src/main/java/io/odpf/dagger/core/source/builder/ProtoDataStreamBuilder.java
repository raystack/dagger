package io.odpf.dagger.core.source.builder;


import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.DataTypes;
import io.odpf.dagger.core.source.StreamConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.odpf.dagger.core.metrics.telemetry.TelemetryTypes.INPUT_PROTO;
import static io.odpf.dagger.core.source.StreamMetrics.addDefaultMetrics;
import static io.odpf.dagger.core.source.StreamMetrics.addMetric;

public class ProtoDataStreamBuilder extends StreamBuilder {
    private final String protoClassName;
    private final StreamConfig streamConfig;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private Configuration configuration;
    private Map<String, List<String>> metrics = new HashMap<>();

    public ProtoDataStreamBuilder(StreamConfig streamConfig, StencilClientOrchestrator stencilClientOrchestrator, Configuration configuration) {
        super(streamConfig, configuration, stencilClientOrchestrator);
        this.streamConfig = streamConfig;
        this.protoClassName = streamConfig.getProtoClass();
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.configuration = configuration;
    }

    @Override
    void addTelemetry() {
        addDefaultMetrics(streamConfig, metrics);
        addMetric(metrics, INPUT_PROTO.getValue(), protoClassName);
    }

    @Override
    Map<String, List<String>> getMetrics() {
        return metrics;
    }

    @Override
    public boolean canBuild() {
        return getInputDataType() == DataTypes.PROTO;
    }
}