package io.odpf.dagger.processors.external.grpc;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import io.odpf.dagger.processors.external.ExternalMetricConfig;
import io.odpf.dagger.processors.external.SchemaConfig;
import io.odpf.dagger.processors.types.StreamDecorator;

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
