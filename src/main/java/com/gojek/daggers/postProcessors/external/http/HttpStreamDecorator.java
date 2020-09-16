package com.gojek.daggers.postProcessors.external.http;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;

import java.util.concurrent.TimeUnit;

public class HttpStreamDecorator implements StreamDecorator {
    private final ColumnNameManager columnNameManager;
    private ExternalMetricConfig externalMetricConfig;
    private String[] inputProtoClasses;
    private HttpSourceConfig httpSourceConfig;
    private StencilClientOrchestrator stencilClientOrchestrator;


    public HttpStreamDecorator(HttpSourceConfig httpSourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                               ColumnNameManager columnNameManager, String[] inputProtoClasses, ExternalMetricConfig externalMetricConfig) {
        this.httpSourceConfig = httpSourceConfig;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.externalMetricConfig = externalMetricConfig;
        this.inputProtoClasses = inputProtoClasses;
    }

    @Override
    public Boolean canDecorate() {
        return httpSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
        httpAsyncConnector.notifySubscriber(externalMetricConfig.getTelemetrySubscriber());
        return AsyncDataStream.orderedWait(inputStream, httpAsyncConnector, httpSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, httpSourceConfig.getCapacity());
    }
}
