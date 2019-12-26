package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.ErrorStatsReporter;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
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
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.utils.Constants.ASHIKO_HTTP_PROCESSOR;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class HttpAsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAsyncConnector.class.getName());
    private AsyncHttpClient httpClient;
    private HttpSourceConfig httpSourceConfig;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;
    private boolean telemetryEnabled;
    private long shutDownPeriod;
    private Descriptors.Descriptor outputDescriptor;
    private MeterStatsManager meterStatsManager;
    private ErrorStatsReporter errorStatsReporter;
    private Map<String, List<String>> metrics = new HashMap<>();

    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, StencilClient stencilClient, ColumnNameManager columnNameManager,
                              boolean telemetryEnabled, long shutDownPeriod) {
        this.httpSourceConfig = httpSourceConfig;
        this.stencilClient = stencilClient;
        this.columnNameManager = columnNameManager;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
    }

    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, StencilClient stencilClient, AsyncHttpClient httpClient,
                              MeterStatsManager meterStatsManager, ColumnNameManager columnNameManager, boolean telemetryEnabled,
                              ErrorStatsReporter errorStatsReporter, long shutDownPeriod) {
        this(httpSourceConfig, stencilClient, columnNameManager, telemetryEnabled, shutDownPeriod);
        this.httpClient = httpClient;
        this.meterStatsManager = meterStatsManager;
        this.errorStatsReporter = errorStatsReporter;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        if (httpSourceConfig.getType() != null) {
            String descriptorType = httpSourceConfig.getType();
            outputDescriptor = stencilClient.get(descriptorType);
        }
        if (meterStatsManager == null) {
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);
        }
        if (errorStatsReporter == null && telemetryEnabled) {
            errorStatsReporter = new ErrorStatsReporter(getRuntimeContext(), shutDownPeriod);
        }
        meterStatsManager.register("external.source.http", ExternalSourceAspects.values());
        if (httpClient == null) {
            httpClient = asyncHttpClient(config().setConnectTimeout(httpSourceConfig.getConnectTimeout()));
        }
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(POST_PROCESSOR_TYPE.getValue(), ASHIKO_HTTP_PROCESSOR);
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
        meterStatsManager.markEvent(CLOSE_CONNECTION_ON_HTTP_CLIENT);
        LOGGER.error("HTTP Connector : Connection closed");
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        try {
            RowManager rowManager = new RowManager(input);
            Object[] bodyVariables = getBodyVariablesValues(rowManager, resultFuture);
            if (StringUtils.isEmpty(httpSourceConfig.getRequestPattern()) || Arrays.asList(bodyVariables).isEmpty()) {
                resultFuture.complete(Collections.singleton(rowManager.getAll()));
                meterStatsManager.markEvent(EMPTY_INPUT);
                return;
            }
            String requestBody = String.format(httpSourceConfig.getRequestPattern(), bodyVariables);
            String endpoint = httpSourceConfig.getEndpoint();
            BoundRequestBuilder postRequest = httpClient
                    .preparePost(endpoint)
                    .setBody(requestBody);
            addCustomHeaders(postRequest);

            HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager, rowManager,
                    columnNameManager, outputDescriptor, resultFuture, errorStatsReporter);
            meterStatsManager.markEvent(ExternalSourceAspects.TOTAL_HTTP_CALLS);
            httpResponseHandler.startTimer();
            postRequest.execute(httpResponseHandler);
        } catch (UnknownFormatConversionException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Request pattern '%s' is invalid", httpSourceConfig.getRequestPattern()));
            if (errorStatsReporter != null) {
                errorStatsReporter.reportFatalException(invalidConfigurationException);
            }
            resultFuture.completeExceptionally(invalidConfigurationException);
        } catch (IllegalFormatException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Request pattern '%s' is incompatible with variable", httpSourceConfig.getRequestPattern()));
            if (errorStatsReporter != null) {
                errorStatsReporter.reportFatalException(invalidConfigurationException);
            }
            resultFuture.completeExceptionally(invalidConfigurationException);
        }

    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        RowManager rowManager = new RowManager(input);
        meterStatsManager.markEvent(ExternalSourceAspects.TIMEOUTS);
        LOGGER.error("HTTP Connector : Timeout");
        Exception timeoutException = new TimeoutException("Timeout in HTTP Call");
        if (httpSourceConfig.isFailOnErrors()) {
            if (errorStatsReporter != null) {
                errorStatsReporter.reportFatalException(timeoutException);
            }
            resultFuture.completeExceptionally(timeoutException);
        } else {
            if (errorStatsReporter != null) {
                errorStatsReporter.reportNonFatalException(timeoutException);
            }
        }
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private Object[] getBodyVariablesValues(RowManager rowManager, ResultFuture<Row> resultFuture) throws InterruptedException {
        List<String> requiredInputColumns = Arrays.asList(httpSourceConfig.getRequestVariables().split(","));
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        for (String inputColumnName : requiredInputColumns) {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            if (inputColumnIndex == -1) {
                meterStatsManager.markEvent(INVALID_CONFIGURATION);
                Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Column '%s' not found as configured in the request variable", inputColumnName));
                if (errorStatsReporter != null) {
                    errorStatsReporter.reportFatalException(invalidConfigurationException);
                }
                resultFuture.completeExceptionally(invalidConfigurationException);
                return new Object[0];
            }
            inputColumnValues.add(rowManager.getFromInput(inputColumnIndex));
        }
        requiredInputColumns.forEach(inputColumnName -> {

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

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

}
