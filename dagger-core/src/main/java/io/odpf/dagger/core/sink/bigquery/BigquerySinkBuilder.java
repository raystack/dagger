package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializerHelper;

public class BigquerySinkBuilder {

    public BigquerySink build() {
        ProtoSerializerHelper protoSerializerHelper = new ProtoSerializerHelper(keyProtoClassName, columnNames, stencilClientOrchestrator, messageProtoClassName);
        return new BigquerySink(protoSerializerHelper, configuration);
    }

    private String keyProtoClassName;
    private String[] columnNames;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private String messageProtoClassName;
    private Configuration configuration;

    public BigquerySinkBuilder setKeyProtoClassName(String keyProtoClassName) {
        this.keyProtoClassName = keyProtoClassName;
        return this;
    }

    public BigquerySinkBuilder setMessageProtoClassName(String messageProtoClassName) {
        this.messageProtoClassName = messageProtoClassName;
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

    public BigquerySinkBuilder setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        return this;
    }
}
