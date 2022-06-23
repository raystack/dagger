package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.depot.config.BigQuerySinkConfig;
import org.aeonbits.owner.ConfigFactory;

public class BigquerySinkBuilder {

    private String[] columnNames;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private Configuration configuration;

    private BigquerySinkBuilder() {
    }

    public static BigquerySinkBuilder create() {
        return new BigquerySinkBuilder();
    }

    public BigquerySink build() {
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, configuration.getParam().toMap());
        ProtoSerializer protoSerializer = new ProtoSerializer(
                sinkConfig.getSinkConnectorSchemaKeyClass(),
                sinkConfig.getSinkConnectorSchemaMessageClass(),
                columnNames,
                stencilClientOrchestrator);
        return new BigquerySink(configuration, protoSerializer);
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

}
