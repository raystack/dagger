package com.gojek.daggers.postProcessors.external.pg;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;

import java.util.concurrent.TimeUnit;

public class PgStreamDecorator implements StreamDecorator {
    private final PgSourceConfig pgSourceConfig;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final ColumnNameManager columnNameManager;
    private final ExternalMetricConfig externalMetricConfig;
    private String[] inputProtoClasses;

    public PgStreamDecorator(PgSourceConfig pgSourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                             ColumnNameManager columnNameManager, String[] inputProtoClasses, ExternalMetricConfig externalMetricConfig) {
        this.pgSourceConfig = pgSourceConfig;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.externalMetricConfig = externalMetricConfig;
        this.inputProtoClasses = inputProtoClasses;
    }

    @Override
    public Boolean canDecorate() {
        return pgSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
        pgAsyncConnector.notifySubscriber(externalMetricConfig.getTelemetrySubscriber());
        return AsyncDataStream.orderedWait(inputStream, pgAsyncConnector, pgSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, pgSourceConfig.getCapacity());
    }
}
