package io.odpf.dagger.core.processors.external.http;

import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.types.StreamDecorator;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

/**
 * The decorator for Http stream.
 */
public class HttpStreamDecorator implements StreamDecorator {

    private final HttpSourceConfig httpSourceConfig;
    private final ExternalMetricConfig externalMetricConfig;
    private final SchemaConfig schemaConfig;

    /**
     * Instantiates a new Http stream decorator.
     *
     * @param httpSourceConfig     the http source config
     * @param externalMetricConfig the external metric config
     * @param schemaConfig         the schema config
     */
    public HttpStreamDecorator(HttpSourceConfig httpSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        this.httpSourceConfig = httpSourceConfig;
        this.externalMetricConfig = externalMetricConfig;
        this.schemaConfig = schemaConfig;
    }

    @Override
    public Boolean canDecorate() {
        return httpSourceConfig != null;
    }

    @Override
    public DataStream<Row> decorate(DataStream<Row> inputStream) {
        HttpAsyncConnector httpAsyncConnector = new HttpAsyncConnector(httpSourceConfig, externalMetricConfig, schemaConfig);
        httpAsyncConnector.notifySubscriber(externalMetricConfig.getTelemetrySubscriber());
        return AsyncDataStream.orderedWait(inputStream, httpAsyncConnector, httpSourceConfig.getStreamTimeout(), TimeUnit.MILLISECONDS, httpSourceConfig.getCapacity());
    }
}
