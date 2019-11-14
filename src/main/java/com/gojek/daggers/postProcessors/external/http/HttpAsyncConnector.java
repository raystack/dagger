package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.metrics.AsyncAspects;
import com.gojek.daggers.metrics.ExternalSourceAspects;
import com.gojek.daggers.metrics.StatsManager;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.ExternalSourceAspects.CLOSE_CONNECTION_ON_HTTP_CLIENT;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class HttpAsyncConnector extends RichAsyncFunction<Row, Row> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAsyncConnector.class.getName());
    private AsyncHttpClient httpClient;
    private HttpSourceConfig httpSourceConfig;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;
    private Descriptors.Descriptor outputDescriptor;
    private StatsManager statsManager;


    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager) {
        this.httpSourceConfig = httpSourceConfig;
        this.stencilClient = stencilClient;
        this.columnNameManager = columnNameManager;
    }

    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, StencilClient stencilClient, AsyncHttpClient httpClient, StatsManager statsManager, ColumnNameManager columnNameManager) {
        this(httpSourceConfig, stencilClient, columnNameManager);
        this.httpClient = httpClient;
        this.statsManager = statsManager;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        if (httpSourceConfig.getType() != null) {
            String descriptorType = httpSourceConfig.getType();
            outputDescriptor = stencilClient.get(descriptorType);
        }
        if (statsManager == null) {
            statsManager = new StatsManager(getRuntimeContext(), true);
        }
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
        RowManager rowManager = new RowManager(input);
        Object[] bodyVariables = getBodyVariablesValues(rowManager);
        String requestBody = String.format(httpSourceConfig.getRequestPattern(), bodyVariables);
        String endpoint = httpSourceConfig.getEndpoint();

        if (StringUtils.isEmpty(requestBody) || Arrays.asList(bodyVariables).isEmpty()) {
            resultFuture.complete(Collections.singleton(rowManager.getAll()));
            statsManager.markEvent(AsyncAspects.EMPTY_INPUT);
            return;
        }
        BoundRequestBuilder postRequest = httpClient
                .preparePost(endpoint)
                .setBody(requestBody);
        addCustomHeaders(postRequest);

        HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, statsManager, rowManager, columnNameManager, outputDescriptor, resultFuture);
        statsManager.markEvent(ExternalSourceAspects.TOTAL_HTTP_CALLS);
        httpResponseHandler.startTimer();
        postRequest.execute(httpResponseHandler);
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        RowManager rowManager = new RowManager(input);
        statsManager.markEvent(ExternalSourceAspects.TIMEOUTS);
        LOGGER.error("HTTP Connector : Timeout");
        if (httpSourceConfig.isFailOnErrors())
            resultFuture.completeExceptionally(new TimeoutException("Timeout in HTTP Call"));
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private AsyncHttpClient createHttpClient() {
        return asyncHttpClient(config().setConnectTimeout(httpSourceConfig.getConnectTimeout()));
    }

    private Object[] getBodyVariablesValues(RowManager rowManager) {
        List<String> requiredInputColumns = Arrays.asList(httpSourceConfig.getRequestVariables().split(","));
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        requiredInputColumns.forEach(inputColumnName -> {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            inputColumnValues.add(rowManager.getFromInput(inputColumnIndex));
        });
        return inputColumnValues.toArray();
    }

    private void addCustomHeaders(BoundRequestBuilder postRequest) {
        Map<String, String> headerMap;
        headerMap = httpSourceConfig.getHeaders();
        headerMap.keySet().forEach(headerKey -> {
            postRequest.addHeader(headerKey, headerMap.get(headerKey));
        });
    }


}
