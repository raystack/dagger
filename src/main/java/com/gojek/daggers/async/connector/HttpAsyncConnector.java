package com.gojek.daggers.async.connector;

import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.gojek.daggers.Constants.*;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class HttpAsyncConnector extends RichAsyncFunction<Row, Row> {

    private AsyncHttpClient httpClient;
    private String[] columnNames;
    private Map<String, Object> configuration;
    private StencilClient stencilClient;
    private String outputProto;
    private Descriptors.Descriptor descriptor;


    public HttpAsyncConnector(String[] columnNames, Map<String, Object> configuration, StencilClient stencilClient, String outputProto) {
        this.columnNames = columnNames;
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        this.outputProto = outputProto;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        this.descriptor = stencilClient.get(outputProto);

        if (httpClient == null) {
            httpClient = createHttpClient();
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
    }

    private AsyncHttpClient createHttpClient() {
        Integer connectTimeout = getIntegerConfig(configuration, ASYNC_IO_HTTP_CONNECT_TIMEOUT_KEY, ASYNC_IO_HTTP_CONNECT_TIMEOUT_DEFAULT);
        return asyncHttpClient(config().setConnectTimeout(connectTimeout));
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        String body = (String) input.getField(Arrays.asList(columnNames).indexOf(configuration.get(EXTERNAL_SOURCE_HTTP_BODY_FIELD_KEY)));
        BoundRequestBuilder postRequest = httpClient
                .preparePost((String) configuration.get(EXTERNAL_SOURCE_HTTP_ENDPOINT_KEY))
                .setBody(body);

        addCustomHeaders(postRequest);

        Row outputRow = createOutputRow(input);

        AsyncHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, configuration, columnNames, descriptor);
        postRequest.execute(httpResponseHandler);
    }

    private Row createOutputRow(Row input) {
        Row row = new Row(columnNames.length);
        for (int index = 0; index < input.getArity(); index++) {
            row.setField(index, input.getField(index));
        }
        return row;
    }


    private void addCustomHeaders(BoundRequestBuilder postRequest) {
        Map<String, String> headerMap = (Map<String, String>) configuration.get(EXTERNAL_SOURCE_HTTP_HEADER_KEY);
        headerMap.keySet().forEach(headerKey -> {
            postRequest.addHeader(headerKey, headerMap.get(headerKey));
        });
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        resultFuture.complete(Collections.singleton(null));
    }

    private Integer getIntegerConfig(Map<String, Object> fieldConfiguration, String key, String defaultValue) {
        return Integer.valueOf((String) fieldConfiguration.getOrDefault(key, defaultValue));
    }

}
