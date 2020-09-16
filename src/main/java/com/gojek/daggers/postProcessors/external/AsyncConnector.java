package com.gojek.daggers.postProcessors.external;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.DescriptorManager;
import com.gojek.daggers.postProcessors.external.common.EndpointHandler;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import com.google.protobuf.Descriptors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map;
import java.util.UnknownFormatConversionException;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.INVALID_CONFIGURATION;
import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.TIMEOUTS;
import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.TOTAL_EXTERNAL_CALLS;
import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.values;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.SOURCE_METRIC_ID;
import static java.util.Collections.singleton;

public abstract class AsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {
    private StencilClientOrchestrator stencilClientOrchestrator;
    private ErrorReporter errorReporter;
    private MeterStatsManager meterStatsManager;
    private ColumnNameManager columnNameManager;
    private String[] inputProtoClasses;
    private ExternalMetricConfig externalMetricConfig;
    private DescriptorManager descriptorManager;
    private SourceConfig sourceConfig;
    private Map<String, List<String>> metrics = new HashMap<>();
    private String sourceType;
    private Descriptors.Descriptor outputDescriptor;
    private EndpointHandler endpointHandler;

    public AsyncConnector(String sourceType, SourceConfig sourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                          ColumnNameManager columnNameManager, String[] inputProtoClasses, ExternalMetricConfig externalMetricConfig) {
        this.sourceType = sourceType;
        this.sourceConfig = sourceConfig;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.inputProtoClasses = inputProtoClasses;
        this.externalMetricConfig = externalMetricConfig;
    }

    protected ErrorReporter getErrorReporter() {
        return errorReporter;
    }

    protected MeterStatsManager getMeterStatsManager() {
        return meterStatsManager;
    }

    protected EndpointHandler getEndpointHandler() {
        return endpointHandler;
    }

    public ColumnNameManager getColumnNameManager() {
        return columnNameManager;
    }

    public void setErrorReporter(ErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
    }

    public void setMeterStatsManager(MeterStatsManager meterStatsManager) {
        this.meterStatsManager = meterStatsManager;
    }

    public void setDescriptorManager(DescriptorManager descriptorManager) {
        this.descriptorManager = descriptorManager;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        if (descriptorManager == null) {
            descriptorManager = new DescriptorManager(stencilClientOrchestrator);
        }

        createClient();

        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory
                    .getErrorReporter(getRuntimeContext(), externalMetricConfig.isTelemetryEnabled(), externalMetricConfig.getShutDownPeriod());
        }
        if (meterStatsManager == null) {
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);
        }
        if (endpointHandler == null) {
            endpointHandler = new EndpointHandler(sourceConfig, meterStatsManager, errorReporter,
                    inputProtoClasses, columnNameManager, descriptorManager);
        }

        String groupKey = SOURCE_METRIC_ID.getValue();
        String groupValue = sourceType + "." + externalMetricConfig.getMetricId();
        meterStatsManager.register(groupKey, groupValue, values());
    }

    protected abstract void createClient();

    protected abstract void process(Row input, ResultFuture<Row> resultFuture) throws Exception;

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

        try {
            process(input, resultFuture);
            meterStatsManager.markEvent(TOTAL_EXTERNAL_CALLS);
        } catch (UnknownFormatConversionException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("pattern config '%s' is invalid", sourceConfig.getPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } catch (IllegalFormatException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("pattern config '%s' is incompatible with the variable config '%s'", sourceConfig.getPattern(), sourceConfig.getVariables()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } catch (InvalidConfigurationException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            reportAndThrowError(resultFuture, e);
        }
    }

    protected void reportAndThrowError(ResultFuture<Row> resultFuture, Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) {
        meterStatsManager.markEvent(TIMEOUTS);
        Exception timeoutException = new TimeoutException("Timeout in external source call!");
        if (sourceConfig.isFailOnErrors()) {
            reportAndThrowError(resultFuture, timeoutException);
        } else {
            errorReporter.reportNonFatalException(timeoutException);
        }
        resultFuture.complete(singleton(input));
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(POST_PROCESSOR_TYPE.getValue(), sourceType);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Descriptors.Descriptor getOutputDescriptor(ResultFuture<Row> resultFuture) {
        if (sourceConfig.getType() != null) {
            try {
                outputDescriptor = descriptorManager.getDescriptor(sourceConfig.getType());
            } catch (DescriptorNotFoundException descriptorNotFound) {
                reportAndThrowError(resultFuture, descriptorNotFound);
            }
        }
        return outputDescriptor;
    }
}
