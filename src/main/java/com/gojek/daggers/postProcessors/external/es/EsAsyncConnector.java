package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.TelemetryPublisher;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
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

import java.util.*;

import static com.gojek.daggers.metrics.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static com.gojek.daggers.utils.Constants.ASHIKO_ES_PROCESSOR;
import static java.util.Collections.singleton;


public class EsAsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {

    private Descriptors.Descriptor outputDescriptor;
    private RestClient esClient;
    private EsSourceConfig esSourceConfig;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;
    private MeterStatsManager meterStatsManager;
    private Map<String, List<String>> metrics = new HashMap<>();

    public EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager) {
        this.esSourceConfig = esSourceConfig;
        this.stencilClient = stencilClient;
        this.columnNameManager = columnNameManager;
    }

    EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager, MeterStatsManager meterStatsManager, RestClient esClient) {
        this(esSourceConfig, stencilClient, columnNameManager);
        this.meterStatsManager = meterStatsManager;
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
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);
            meterStatsManager.register(groupName, values());
        });
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(POST_PROCESSOR_TYPE.getValue(), ASHIKO_ES_PROCESSOR);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        try {
            RowManager rowManager = new RowManager(input);
            Object[] endpointVariablesValues = getEndpointVariablesValues(rowManager, resultFuture);
            if (isEndpointInvalid(resultFuture, rowManager, endpointVariablesValues)) return;
            String esEndpoint = String.format(esSourceConfig.getEndpointPattern(), endpointVariablesValues);
            Request esRequest = new Request("GET", esEndpoint);
            meterStatsManager.markEvent(TOTAL_ES_CALLS);
            EsResponseHandler esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, outputDescriptor, resultFuture);
            esResponseHandler.startTimer();
            esClient.performRequestAsync(esRequest, esResponseHandler);
        } catch (UnknownFormatConversionException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            resultFuture.completeExceptionally(new InvalidConfigurationException(String.format("Endpoint pattern '%s' is invalid", esSourceConfig.getEndpointPattern())));
        } catch (IllegalFormatException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            resultFuture.completeExceptionally(new InvalidConfigurationException(String.format("Endpoint pattern '%s' is incompatible with the endpoint variable", esSourceConfig.getEndpointPattern())));
        }
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        meterStatsManager.markEvent(TIMEOUTS);
        resultFuture.complete(singleton(input));
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
            meterStatsManager.markEvent(ExternalSourceAspects.EMPTY_INPUT);
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
                meterStatsManager.markEvent(INVALID_CONFIGURATION);
                resultFuture.completeExceptionally(new InvalidConfigurationException(String.format("Column '%s' not found as configured in the endpoint variable", inputColumnName)));
                return new Object[0];
            }
            Object inputColumnValue = rowManager.getFromInput(inputColumnIndex);
            if (inputColumnValue != null && StringUtils.isNotEmpty(String.valueOf(inputColumnValue)))
                inputColumnValues.add(inputColumnValue);
        }

        return inputColumnValues.toArray();
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
