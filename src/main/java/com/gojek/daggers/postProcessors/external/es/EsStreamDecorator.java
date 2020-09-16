package com.gojek.daggers.postProcessors.external.es;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;

import java.util.concurrent.TimeUnit;

public class EsStreamDecorator implements StreamDecorator {
    private ExternalMetricConfig externalMetricConfig;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private EsSourceConfig esSourceConfig;
    private ColumnNameManager columnNameManager;
    private String[] inputProtoClasses;


    public EsStreamDecorator(EsSourceConfig esSourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                             ColumnNameManager columnNameManager, String[] inputProtoClasses, ExternalMetricConfig externalMetricConfig) {
        this.esSourceConfig = esSourceConfig;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.externalMetricConfig = externalMetricConfig;
        this.inputProtoClasses = inputProtoClasses;
    }

    @Override
    public Boolean canDecorate() {
        return esSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
        esAsyncConnector.notifySubscriber(externalMetricConfig.getTelemetrySubscriber());
        return AsyncDataStream.orderedWait(inputStream, esAsyncConnector, esSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, esSourceConfig.getCapacity());
    }
}
