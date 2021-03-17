package com.gojek.daggers.processors.external.grpc;

import com.gojek.daggers.processors.external.ExternalMetricConfig;
import com.gojek.daggers.processors.external.SchemaConfig;
import com.gojek.daggers.processors.types.StreamDecorator;
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
