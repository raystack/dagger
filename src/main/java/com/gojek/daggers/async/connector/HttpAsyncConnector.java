package com.gojek.daggers.async.connector;

import com.gojek.daggers.async.metric.ExternalSourceAspects;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.async.metric.ExternalSourceAspects.CLOSE_CONNECTION_ON_HTTP_CLIENT;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class HttpAsyncConnector extends RichAsyncFunction<Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAsyncConnector.class.getName());
    private AsyncHttpClient httpClient;
    private String[] columnNames;
    private HttpExternalSourceConfig httpExternalSourceConfig;
    private StencilClient stencilClient;
    private String outputProto;
    private Descriptors.Descriptor descriptor;
    private StatsManager statsManager;


    public HttpAsyncConnector(String[] columnNames, HttpExternalSourceConfig httpExternalSourceConfig, StencilClient stencilClient, String outputProto) {
        this.columnNames = columnNames;
        this.httpExternalSourceConfig = httpExternalSourceConfig;
        this.stencilClient = stencilClient;
        this.outputProto = outputProto;
    }

    public HttpAsyncConnector(String[] columnNames, HttpExternalSourceConfig httpExternalSourceConfig, StencilClient stencilClient, String outputProto, AsyncHttpClient httpClient, StatsManager statsManager) {
        this(columnNames, httpExternalSourceConfig, stencilClient, outputProto);
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
        String bodyColumnName = httpExternalSourceConfig.getBodyField();
        String endpoint = httpExternalSourceConfig.getEndpoint();
        validateInputs(bodyColumnName, resultFuture);
        String body = (String) input.getField(Arrays.asList(columnNames).indexOf(bodyColumnName));

        BoundRequestBuilder postRequest = httpClient
                .preparePost(endpoint)
                .setBody(body);
        addCustomHeaders(postRequest);

        Row outputRow = createOutputRow(input);
        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(outputRow, resultFuture, httpExternalSourceConfig, columnNames, descriptor, statsManager);
        statsManager.markEvent(ExternalSourceAspects.TOTAL_HTTP_CALLS);
        httpResponseHandler.start();
        postRequest.execute(httpResponseHandler);
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        statsManager.markEvent(ExternalSourceAspects.TIMEOUTS);
        LOGGER.error("HTTP Connector : Timeout");
        if (httpExternalSourceConfig.isFailOnErrors())
            resultFuture.completeExceptionally(new TimeoutException("Timeout in HTTP Call"));
        resultFuture.complete(Collections.singleton(createOutputRow(input)));
    }

    private Row createOutputRow(Row input) {
        Row row = new Row(columnNames.length);
        for (int index = 0; index < input.getArity(); index++) {
            row.setField(index, input.getField(index));
        }
        return row;
    }

    private AsyncHttpClient createHttpClient() {
        return asyncHttpClient(config().setConnectTimeout(Integer.parseInt(httpExternalSourceConfig.getConnectTimeout())));
    }

    private void addCustomHeaders(BoundRequestBuilder postRequest) {
        Map<String, String> headerMap;
        headerMap = httpExternalSourceConfig.getHeaders();
        headerMap.keySet().forEach(headerKey -> {
            postRequest.addHeader(headerKey, headerMap.get(headerKey));
        });
    }

    private void validateInputs(String bodyColumnName, ResultFuture<Row> resultFuture) {
        if (!Arrays.asList(columnNames).contains(bodyColumnName)) {
            resultFuture.completeExceptionally(new IllegalArgumentException("Request body should be selected through SQL Query"));
        }
    }

}
