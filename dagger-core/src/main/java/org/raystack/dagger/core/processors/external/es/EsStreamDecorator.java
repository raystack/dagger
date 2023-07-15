package org.raystack.dagger.core.processors.external.es;

import org.raystack.dagger.core.processors.common.SchemaConfig;
import org.raystack.dagger.core.processors.types.StreamDecorator;
import org.raystack.dagger.core.processors.external.ExternalMetricConfig;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * The decorator for ElasticSearch stream.
 */
public class EsStreamDecorator implements StreamDecorator {

    private final EsSourceConfig esSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;
    private final SchemaConfig schemaConfig;

    /**
     * Instantiates a new ElasticSearch stream decorator.
     *
     * @param esSourceConfig       the es source config
     * @param externalMetricConfig the external metric config
     * @param schemaConfig         the schema config
     */
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
