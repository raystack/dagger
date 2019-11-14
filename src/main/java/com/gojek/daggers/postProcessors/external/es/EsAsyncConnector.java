package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.AsyncAspects;
import com.gojek.daggers.metrics.StatsManager;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.external.deprecated.ResponseBuilder;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.gojek.daggers.metrics.AsyncAspects.*;
import static java.util.Collections.singleton;


public class EsAsyncConnector extends RichAsyncFunction<Row, Row> {

    private Descriptors.Descriptor outputDescriptor;
    private RestClient esClient;
    private EsSourceConfig esSourceConfig;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;
    private StatsManager statsManager;

    public EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager) {
        this.esSourceConfig = esSourceConfig;
        this.stencilClient = stencilClient;
        this.columnNameManager = columnNameManager;
    }

    EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager, StatsManager statsManager, RestClient esClient) {
        this(esSourceConfig, stencilClient, columnNameManager);
        this.statsManager = statsManager;
        this.esClient = esClient;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (esSourceConfig.hasType()) {
            String descriptorType = esSourceConfig.getType();
            outputDescriptor = stencilClient.get(descriptorType);
        }
        if (esClient == null) {
            esClient = createESClient();
        }
        List<String> esOutputColumnNames = esSourceConfig.getOutputColumns();
        esOutputColumnNames.forEach(outputField -> {
            String groupName = "es." + esOutputColumnNames;
            statsManager = new StatsManager(getRuntimeContext(), true);
            statsManager.register(groupName, values());
        });
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        RowManager rowManager = new RowManager(input);
        Object[] endpointVariablesValues = getEndpointVariablesValues(rowManager, resultFuture);
        if (isEndpointInvalid(resultFuture, rowManager, endpointVariablesValues)) return;
        String esEndpoint = String.format(esSourceConfig.getEndpointPattern(), endpointVariablesValues);
        Request esRequest = new Request("GET", esEndpoint);
        statsManager.markEvent(TOTAL_ES_CALLS);
        EsResponseHandler esResponseHandler = new EsResponseHandler(esSourceConfig, statsManager, rowManager, columnNameManager, outputDescriptor, resultFuture);
        esResponseHandler.startTimer();
        esClient.performRequestAsync(esRequest, esResponseHandler);
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        statsManager.markEvent(TIMEOUTS);
        resultFuture.complete(singleton(new ResponseBuilder(input).build()));
    }

    private RestClient createESClient() {
        return RestClient.builder(
                getHttpHosts()
        ).setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                        .setConnectTimeout(esSourceConfig.getConnectTimeout())
                        .setSocketTimeout(esSourceConfig.getSocketTimeout()))
                .setMaxRetryTimeoutMillis(esSourceConfig.getRetryTimeout()).build();
    }

    private HttpHost[] getHttpHosts() {
        List<String> hosts = Arrays.asList(esSourceConfig.getHost().split(","));
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        hosts.forEach(s -> httpHosts.add(new HttpHost(s, esSourceConfig.getPort())));
        return httpHosts.toArray(new HttpHost[0]);
    }

    private boolean isEndpointInvalid(ResultFuture<Row> resultFuture, RowManager rowManager, Object[] endpointVariablesValues) {
        boolean invalidEndpointPattern = StringUtils.isEmpty(esSourceConfig.getEndpointPattern());
        boolean emptyEndpointVariable = Arrays.asList(endpointVariablesValues).isEmpty();
        if (invalidEndpointPattern || emptyEndpointVariable) {
            statsManager.markEvent(AsyncAspects.EMPTY_INPUT);
            resultFuture.complete(singleton(rowManager.getAll()));
            return true;
        }
        return false;
    }

    private Object[] getEndpointVariablesValues(RowManager rowManager, ResultFuture<Row> resultFuture) {
        List<String> requiredInputColumns = Arrays.asList(esSourceConfig.getEndpointVariables().split(","));
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        for (String inputColumnName : requiredInputColumns) {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            if (inputColumnIndex == -1) {
                statsManager.markEvent(INVALID_CONFIGURATION);
                resultFuture.completeExceptionally(new InvalidConfigurationException(String.format("Column '%s' not found as configured in the endpoint variable", inputColumnName)));
                return new Object[0];
            }
            Object inputColumnValue = rowManager.getFromInput(inputColumnIndex);
            if (inputColumnValue != null && StringUtils.isNotEmpty(String.valueOf(inputColumnValue)))
                inputColumnValues.add(inputColumnValue);
        }

        return inputColumnValues.toArray();
    }
}
