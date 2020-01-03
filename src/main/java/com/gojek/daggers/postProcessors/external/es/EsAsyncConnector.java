package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.utils.Constants.ASHIKO_ES_PROCESSOR;
import static java.util.Collections.singleton;


public class EsAsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(EsAsyncConnector.class.getName());
    private Descriptors.Descriptor outputDescriptor;
    private RestClient esClient;
    private boolean telemetryEnabled;
    private EsSourceConfig esSourceConfig;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;
    private long shutDownPeriod;
    private MeterStatsManager meterStatsManager;
    private Map<String, List<String>> metrics = new HashMap<>();
    private ErrorReporter errorReporter;

    public EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager,
                            boolean telemetryEnabled, long shutDownPeriod) {
        this.esSourceConfig = esSourceConfig;
        this.stencilClient = stencilClient;
        this.columnNameManager = columnNameManager;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
    }

    EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager,
                     MeterStatsManager meterStatsManager, RestClient esClient, boolean telemetryEnabled, ErrorReporter errorReporter, long shutDownPeriod) {
        this(esSourceConfig, stencilClient, columnNameManager, telemetryEnabled, shutDownPeriod);
        this.meterStatsManager = meterStatsManager;
        this.esClient = esClient;
        this.errorReporter = errorReporter;
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
        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext(), telemetryEnabled, shutDownPeriod);
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
            EsResponseHandler esResponseHandler = new EsResponseHandler(esSourceConfig, meterStatsManager, rowManager, columnNameManager, outputDescriptor, resultFuture, errorReporter);
            esResponseHandler.startTimer();
            esClient.performRequestAsync(esRequest, esResponseHandler);
        } catch (UnknownFormatConversionException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Endpoint pattern '%s' is invalid", esSourceConfig.getEndpointPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } catch (IllegalFormatException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Endpoint pattern '%s' is incompatible with the endpoint variable", esSourceConfig.getEndpointPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        }
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        meterStatsManager.markEvent(TIMEOUTS);
        LOGGER.error("HTTP Connector : Timeout");
        Exception timeoutException = new TimeoutException("Timeout in Elastic Search Call");
        errorReporter.reportNonFatalException(timeoutException);
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
                Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Column '%s' not found as configured in the endpoint variable", inputColumnName));
                reportAndThrowError(resultFuture, invalidConfigurationException);
                return new Object[0];
            }
            Object inputColumnValue = rowManager.getFromInput(inputColumnIndex);
            if (inputColumnValue != null && StringUtils.isNotEmpty(String.valueOf(inputColumnValue)))
                inputColumnValues.add(inputColumnValue);
        }

        return inputColumnValues.toArray();
    }

    private void reportAndThrowError(ResultFuture<Row> resultFuture, Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
