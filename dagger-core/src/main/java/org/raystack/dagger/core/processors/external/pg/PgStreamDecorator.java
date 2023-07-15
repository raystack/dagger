package org.raystack.dagger.core.processors.external.pg;

import org.raystack.dagger.core.processors.common.SchemaConfig;
import org.raystack.dagger.core.processors.types.StreamDecorator;
import org.raystack.dagger.core.processors.external.ExternalMetricConfig;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * The Decorator for Postgre stream.
 */
public class PgStreamDecorator implements StreamDecorator {


    private final PgSourceConfig pgSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;
    private final SchemaConfig schemaConfig;

    /**
     * Instantiates a new Postgre stream decorator.
     *
     * @param pgSourceConfig       the pg source config
     * @param externalMetricConfig the external metric config
     * @param schemaConfig         the schema config
     */
    public PgStreamDecorator(PgSourceConfig pgSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        this.pgSourceConfig = pgSourceConfig;
        this.externalMetricConfig = externalMetricConfig;
        this.schemaConfig = schemaConfig;
    }

    @Override
    public Boolean canDecorate() {
        return pgSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        PgAsyncConnector pgAsyncConnector = new PgAsyncConnector(pgSourceConfig, externalMetricConfig, schemaConfig);
        pgAsyncConnector.notifySubscriber(externalMetricConfig.getTelemetrySubscriber());
        return AsyncDataStream.orderedWait(inputStream, pgAsyncConnector, pgSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, pgSourceConfig.getCapacity());
    }
}
