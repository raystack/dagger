package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.exception.InvalidHttpVerbException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.external.http.request.HttpRequestFactory;
import com.gojek.de.stencil.client.StencilClient;
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
    private StencilClientOrchestrator stencilClientOrchestrator;
    private StencilClient stencilClient;
    private ColumnNameManager columnNameManager;
    private boolean telemetryEnabled;
    private long shutDownPeriod;
    private Descriptors.Descriptor outputDescriptor;
    private MeterStatsManager meterStatsManager;
    private ErrorReporter errorReporter;
    private Map<String, List<String>> metrics = new HashMap<>();

    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, StencilClientOrchestrator stencilClient, ColumnNameManager columnNameManager,
                              boolean telemetryEnabled, long shutDownPeriod) {
        this.httpSourceConfig = httpSourceConfig;
        this.stencilClientOrchestrator = stencilClient;
        this.columnNameManager = columnNameManager;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
    }

    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, StencilClientOrchestrator stencilClientOrchestrator, AsyncHttpClient httpClient,
                              MeterStatsManager meterStatsManager, ColumnNameManager columnNameManager, boolean telemetryEnabled,
                              ErrorReporter errorReporter, long shutDownPeriod, StencilClient stencilClient) {
        this(httpSourceConfig, stencilClientOrchestrator, columnNameManager, telemetryEnabled, shutDownPeriod);
        this.httpClient = httpClient;
        this.meterStatsManager = meterStatsManager;
        this.errorReporter = errorReporter;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        if (stencilClient == null) {
            stencilClient = stencilClientOrchestrator.getStencilClient();
        }
        if (meterStatsManager == null) {
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);
        }
        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext(), telemetryEnabled, shutDownPeriod);
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
        httpClient = null;
        meterStatsManager.markEvent(CLOSE_CONNECTION_ON_HTTP_CLIENT);
        LOGGER.error("HTTP Connector : Connection closed");
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        try {
            RowManager rowManager = new RowManager(input);

            Object[] requestVariablesValues = getRequestVariablesValues(rowManager, resultFuture);

            if (StringUtils.isEmpty(httpSourceConfig.getRequestPattern()) || Arrays.asList(requestVariablesValues).isEmpty()) {
                resultFuture.complete(Collections.singleton(rowManager.getAll()));
                meterStatsManager.markEvent(EMPTY_INPUT);
                return;
            }
            BoundRequestBuilder request = HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues);
            HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, meterStatsManager,
                    rowManager, columnNameManager, getOutputDescriptor(resultFuture), resultFuture, errorReporter);
            meterStatsManager.markEvent(ExternalSourceAspects.TOTAL_HTTP_CALLS);
            httpResponseHandler.startTimer();
            request.execute(httpResponseHandler);
        } catch (InvalidHttpVerbException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            resultFuture.completeExceptionally(e);
        } catch (UnknownFormatConversionException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Request pattern '%s' is invalid", httpSourceConfig.getRequestPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } catch (IllegalFormatException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Request pattern '%s' is incompatible with variable", httpSourceConfig.getRequestPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        }

    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        RowManager rowManager = new RowManager(input);
        meterStatsManager.markEvent(ExternalSourceAspects.TIMEOUTS);
        LOGGER.error("HTTP Connector : Timeout");
        Exception timeoutException = new TimeoutException("Timeout in HTTP Call");
        if (httpSourceConfig.isFailOnErrors()) {
            reportAndThrowError(resultFuture, timeoutException);
        } else {
            errorReporter.reportNonFatalException(timeoutException);
        }
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private Object[] getRequestVariablesValues(RowManager rowManager, ResultFuture<Row> resultFuture) {
        List<String> requiredInputColumns = Arrays.asList(httpSourceConfig.getRequestVariables().split(","));
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        for (String inputColumnName : requiredInputColumns) {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            if (inputColumnIndex == -1) {
                meterStatsManager.markEvent(INVALID_CONFIGURATION);
                Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Column '%s' not found as configured in the request variable", inputColumnName));
                reportAndThrowError(resultFuture, invalidConfigurationException);
                return new Object[0];
            }
            inputColumnValues.add(rowManager.getFromInput(inputColumnIndex));
        }
        requiredInputColumns.forEach(inputColumnName -> {

        });
        return inputColumnValues.toArray();
    }

    private Descriptors.Descriptor getOutputDescriptor(ResultFuture<Row> resultFuture) {
        if (httpSourceConfig.getType() != null) {
            String descriptorType = httpSourceConfig.getType();
            outputDescriptor = stencilClient.get(descriptorType);
            if (outputDescriptor == null) {
                reportAndThrowError(resultFuture, new DescriptorNotFoundException("No Descriptor found for class " + descriptorType));
            }
        }
        return outputDescriptor;
    }

    private void reportAndThrowError(ResultFuture<Row> resultFuture, Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }
}
