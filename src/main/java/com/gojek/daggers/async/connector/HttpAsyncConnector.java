package com.gojek.daggers.async.connector;

import com.gojek.daggers.async.metric.ExternalSourceAspects;
import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.gojek.daggers.Constants.*;
import static com.gojek.daggers.async.metric.ExternalSourceAspects.CLOSE_CONNECTION_ON_HTTP_CLIENT;
import static com.gojek.daggers.longbow.metric.LongbowReaderAspects.CLOSE_CONNECTION_ON_READER;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class HttpAsyncConnector extends RichAsyncFunction<Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAsyncConnector.class.getName());
    private AsyncHttpClient httpClient;
    private String[] columnNames;
    private Map<String, Object> configuration;
    private StencilClient stencilClient;
    private String outputProto;
    private Descriptors.Descriptor descriptor;
    private StatsManager statsManager;


    public HttpAsyncConnector(String[] columnNames, Map<String, Object> configuration, StencilClient stencilClient, String outputProto) {
        this.columnNames = columnNames;
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        this.outputProto = outputProto;
    }

    public HttpAsyncConnector(String[] columnNames, Map<String, Object> configuration, StencilClient stencilClient, String outputProto, AsyncHttpClient httpClient, StatsManager statsManager) {
        this(columnNames, configuration, stencilClient, outputProto);
        this.httpClient = httpClient;
        this.statsManager = statsManager;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        this.descriptor = stencilClient.get(outputProto);

        if (statsManager == null)
            statsManager = new StatsManager(getRuntimeContext(), true);
        statsManager.register("external.source.http", ExternalSourceAspects.values());

        if (httpClient == null) {
            httpClient = createHttpClient();
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
        statsManager.markEvent(CLOSE_CONNECTION_ON_HTTP_CLIENT);
        LOGGER.error("HTTP Connector : Connection closed");
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        String bodyColumnName = (String) configuration.get(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY);
        String endpoint = (String) configuration.get(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY);
        validateInputs(bodyColumnName, endpoint);
        String body = (String) input.getField(Arrays.asList(columnNames).indexOf(bodyColumnName));

        BoundRequestBuilder postRequest = httpClient
                .preparePost(endpoint)
                .setBody(body);
        addCustomHeaders(postRequest);

        Row outputRow = createOutputRow(input);
        AsyncHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor, statsManager);
        statsManager.markEvent(ExternalSourceAspects.TOTAL_HTTP_CALLS);
        postRequest.execute(httpResponseHandler);
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        statsManager.markEvent(ExternalSourceAspects.TIMEOUTS);
        LOGGER.error("HTTP Connector : Timeout");
        if ((boolean) configuration.getOrDefault(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, EXTERNAL_SOURCE_ENABLED_KEY_DEFAULT))
            throw new RuntimeException("Timeout in HTTP Call");
        resultFuture.complete(Collections.singleton(input));
    }

    private Row createOutputRow(Row input) {
        Row row = new Row(columnNames.length);
        for (int index = 0; index < input.getArity(); index++) {
            row.setField(index, input.getField(index));
        }
        return row;
    }

    private AsyncHttpClient createHttpClient() {
        Integer connectTimeout = getIntegerConfig(configuration, ASYNC_IO_HTTP_CONNECT_TIMEOUT_KEY, ASYNC_IO_HTTP_CONNECT_TIMEOUT_DEFAULT);
        return asyncHttpClient(config().setConnectTimeout(connectTimeout));
    }

    private void addCustomHeaders(BoundRequestBuilder postRequest) {
        Map<String, String> headerMap;
        try {
            headerMap = (Map<String, String>) configuration.get(EXTERNAL_SOURCE_HTTP_HEADER_KEY);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Config for header should be a map");
        }
        if (headerMap == null)
            return;
        headerMap.keySet().forEach(headerKey -> {
            postRequest.addHeader(headerKey, headerMap.get(headerKey));
        });
    }

    private void validateInputs(String bodyColumnName, String endpoint) {
        if (bodyColumnName == null) {
            throw new IllegalArgumentException("Request body key should be passed");
        }
        if (!Arrays.asList(columnNames).contains(bodyColumnName)) {
            throw new IllegalArgumentException("Request body should be selected through SQL Query");
        }
        if (endpoint == null) {
            throw new IllegalArgumentException("Http Endpoint should be provided");
        }
    }

    private Integer getIntegerConfig(Map<String, Object> fieldConfiguration, String key, String defaultValue) {
        return Integer.valueOf((String) fieldConfiguration.getOrDefault(key, defaultValue));
    }

}
