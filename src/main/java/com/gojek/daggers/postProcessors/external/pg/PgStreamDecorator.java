package com.gojek.daggers.postProcessors.external.pg;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class PgStreamDecorator implements StreamDecorator {
    private final PgSourceConfig pgSourceConfig;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final ColumnNameManager columnNameManager;
    private final TelemetrySubscriber telemetrySubscriber;
    private final boolean telemetryEnabled;
    private final long shutDownPeriod;

    public PgStreamDecorator(PgSourceConfig pgSourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                             ColumnNameManager columnNameManager, TelemetrySubscriber telemetrySubscriber, boolean telemetryEnabled, long shutDownPeriod) {
        this.pgSourceConfig = pgSourceConfig;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.telemetrySubscriber = telemetrySubscriber;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
    }

    @Override
    public Boolean canDecorate() {
        return pgSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, stencilClientOrchestrator, columnNameManager, telemetryEnabled, shutDownPeriod);
        pgAsyncConnector.notifySubscriber(telemetrySubscriber);
        return AsyncDataStream.orderedWait(inputStream, pgAsyncConnector, pgSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, pgSourceConfig.getCapacity());
    }
}
