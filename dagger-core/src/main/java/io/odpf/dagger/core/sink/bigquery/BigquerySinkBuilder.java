package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.depot.bigquery.BigQuerySinkFactory;

public class BigquerySinkBuilder {

    private String schemaKeyClass;
    private String schemaMessageClass;
    private String[] columnNames;
    private int batchSize;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private BigQuerySinkFactory sinkConnectorFactory;

    private BigquerySinkBuilder() {
    }

    public static BigquerySinkBuilder create() {
        return new BigquerySinkBuilder();
    }

    public BigquerySink build() {
        ProtoSerializer protoSerializer = new ProtoSerializer(
                schemaKeyClass,
                columnNames,
                stencilClientOrchestrator,
                schemaMessageClass);
        return new BigquerySink(batchSize, protoSerializer, sinkConnectorFactory);
    }

    public BigquerySinkBuilder setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public BigquerySinkBuilder setSchemaKeyClass(String schemaKeyClass) {
        this.schemaKeyClass = schemaKeyClass;
        return this;
    }

    public BigquerySinkBuilder setSchemaMessageClass(String schemaMessageClass) {
        this.schemaMessageClass = schemaMessageClass;
        return this;
    }

    public BigquerySinkBuilder setSinkConnectorFactory(BigQuerySinkFactory factory) {
        this.sinkConnectorFactory = factory;
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
