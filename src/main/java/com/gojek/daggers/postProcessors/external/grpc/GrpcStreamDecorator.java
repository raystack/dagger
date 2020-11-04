package com.gojek.daggers.postProcessors.external.grpc;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.telemetry.TelemetrySubscriber;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.SchemaConfig;
import com.gojek.daggers.postProcessors.external.common.StreamDecorator;
import com.gojek.daggers.postProcessors.external.http.HttpAsyncConnector;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import com.gojek.daggers.postProcessors.external.pg.PgAsyncConnector;
import com.gojek.daggers.postProcessors.external.pg.PgSourceConfig;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class GrpcStreamDecorator implements StreamDecorator {

    private GrpcSourceConfig grpcSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;
    private final SchemaConfig schemaConfig;


    public GrpcStreamDecorator(GrpcSourceConfig grpcSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        this.grpcSourceConfig = grpcSourceConfig;
        this.externalMetricConfig = externalMetricConfig;
        this.schemaConfig = schemaConfig;
    }


    @Override
    public Boolean canDecorate() {
        return grpcSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        GrpcAsyncConnector grpcAsyncConnector = new GrpcAsyncConnector(grpcSourceConfig, externalMetricConfig, schemaConfig);
        grpcAsyncConnector.notifySubscriber(externalMetricConfig.getTelemetrySubscriber());
        return AsyncDataStream.orderedWait(inputStream, grpcAsyncConnector, grpcSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, grpcSourceConfig.getCapacity());
    }
}
