package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.dagger.core.metrics.reporters.statsd.DaggerStatsDReporter;
import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;

public class BigquerySinkBuilder {

    private String[] columnNames;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private Configuration configuration;
    private DaggerStatsDReporter daggerStatsDReporter;

    private BigquerySinkBuilder() {
    }

    public static BigquerySinkBuilder create() {
        return new BigquerySinkBuilder();
    }

    public BigquerySink build() {
        ProtoSerializer protoSerializer = new ProtoSerializer(
                configuration.getString("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS", ""),
                configuration.getString("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS", ""),
                columnNames,
                stencilClientOrchestrator);
        Configuration conf = setDefaultValues(configuration);
        return new BigquerySink(conf, protoSerializer, daggerStatsDReporter);
    }

    private Configuration setDefaultValues(Configuration inputConf) {
        Map<String, String> configMap = new HashMap<>(inputConf.getParam().toMap());
        configMap.put("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH", "false");
        configMap.put("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS", "86400000");
        configMap.put("SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES", "4");
        configMap.put("SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS", "5000");
        configMap.put("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY", "LONG_POLLING");
        configMap.put("SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS", "60000");
        configMap.put("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS", "");
        configMap.put("SINK_METRICS_APPLICATION_PREFIX", "dagger_");
        configMap.put("SINK_BIGQUERY_ROW_INSERT_ID_ENABLE", "false");
        return new Configuration(ParameterTool.fromMap(configMap));
    }

    public BigquerySinkBuilder setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    public BigquerySinkBuilder setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
        return this;
    }

    public BigquerySinkBuilder setStencilClientOrchestrator(StencilClientOrchestrator stencilClientOrchestrator) {
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        return this;
    }

    public BigquerySinkBuilder setDaggerStatsDReporter(DaggerStatsDReporter daggerStatsDReporter) {
        this.daggerStatsDReporter = daggerStatsDReporter;
        return this;
    }
}
