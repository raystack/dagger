package io.odpf.dagger.processors.external.es;

import io.odpf.dagger.processors.external.ExternalMetricConfig;
import io.odpf.dagger.processors.external.SchemaConfig;
import io.odpf.dagger.processors.types.StreamDecorator;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class EsStreamDecorator implements StreamDecorator {

    private final EsSourceConfig esSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;
    private final SchemaConfig schemaConfig;

    public EsStreamDecorator(EsSourceConfig esSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        this.esSourceConfig = esSourceConfig;
        this.externalMetricConfig = externalMetricConfig;
        this.schemaConfig = schemaConfig;
    }

    @Override
    public Boolean canDecorate() {
        return esSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        EsAsyncConnector esAsyncConnector = new EsAsyncConnector(esSourceConfig, externalMetricConfig, schemaConfig);
        esAsyncConnector.notifySubscriber(externalMetricConfig.getTelemetrySubscriber());
        return AsyncDataStream.orderedWait(inputStream, esAsyncConnector, esSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, esSourceConfig.getCapacity());
    }
}
