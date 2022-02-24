package io.odpf.dagger.core.processors.external.grpc;

import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.types.StreamDecorator;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * The Decorator for Grpc stream.
 */
public class GrpcStreamDecorator implements StreamDecorator {

    private GrpcSourceConfig grpcSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;
    private final SchemaConfig schemaConfig;


    /**
     * Instantiates a new Grpc stream decorator.
     *
     * @param grpcSourceConfig     the grpc source config
     * @param externalMetricConfig the external metric config
     * @param schemaConfig         the schema config
     */
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
