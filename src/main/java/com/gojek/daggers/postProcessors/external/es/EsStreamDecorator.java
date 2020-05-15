package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class EsStreamDecorator implements StreamDecorator {
    private String metricId;
    private StencilClientOrchestrator stencilClientOrchestrator;
    private EsSourceConfig esSourceConfig;
    private ColumnNameManager columnNameManager;
    private TelemetrySubscriber telemetrySubscriber;
    private boolean telemetryEnabled;
    private long shutDownPeriod;


    public EsStreamDecorator(EsSourceConfig esSourceConfig, String metricId, StencilClientOrchestrator stencilClientOrchestrator,
                             ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber, boolean telemetryEnabled, long shutDownPeriod) {
        this.esSourceConfig = esSourceConfig;
        this.metricId = metricId;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.telemetrySubscriber = telemetrySubscriber;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
    }

    @Override
    public Boolean canDecorate() {
        return esSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, telemetryEnabled, shutDownPeriod);
        esAsyncConnector.notifySubscriber(telemetrySubscriber);
        return AsyncDataStream.orderedWait(inputStream, esAsyncConnector, esSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, esSourceConfig.getCapacity());
    }
}
