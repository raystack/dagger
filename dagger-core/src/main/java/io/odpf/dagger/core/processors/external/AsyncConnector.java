package io.odpf.dagger.core.processors.external;

import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.common.EndpointHandler;
import io.odpf.dagger.core.processors.types.SourceConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.exception.DescriptorNotFoundException;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.metrics.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map;
import java.util.UnknownFormatConversionException;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;

public abstract class AsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {
    private final String sourceType;
    private final SourceConfig sourceConfig;
    private final ExternalMetricConfig externalMetricConfig;
    private final SchemaConfig schemaConfig;
    private ErrorReporter errorReporter;
    private MeterStatsManager meterStatsManager;
    private DescriptorManager descriptorManager;
    private Map<String, List<String>> metrics = new HashMap<>();
    private Descriptors.Descriptor outputDescriptor;
    private EndpointHandler endpointHandler;

    public AsyncConnector(String sourceType, SourceConfig sourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        this.sourceType = sourceType;
        this.sourceConfig = sourceConfig;
        this.externalMetricConfig = externalMetricConfig;
        this.schemaConfig = schemaConfig;
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
        return schemaConfig.getColumnNameManager();
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

    public DescriptorManager getDescriptorManager() {
        return descriptorManager;
    }

    protected DescriptorManager initDescriptorManager(SchemaConfig schemaConfig) {
        return new DescriptorManager(schemaConfig.getStencilClientOrchestrator());
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        if (descriptorManager == null) {
            descriptorManager = initDescriptorManager(schemaConfig);
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
                    schemaConfig.getInputProtoClasses(), schemaConfig.getColumnNameManager(), descriptorManager);
        }

        String groupKey = TelemetryTypes.SOURCE_METRIC_ID.getValue();
        String groupValue = sourceType + "." + externalMetricConfig.getMetricId();
        meterStatsManager.register(groupKey, groupValue, ExternalSourceAspects.values());
    }

    protected abstract void createClient();

    protected abstract void process(Row input, ResultFuture<Row> resultFuture) throws Exception;

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

        try {
            process(input, resultFuture);
            meterStatsManager.markEvent(ExternalSourceAspects.TOTAL_EXTERNAL_CALLS);
        } catch (UnknownFormatConversionException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("pattern config '%s' is invalid", sourceConfig.getPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } catch (IllegalFormatException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("pattern config '%s' is incompatible with the variable config '%s'", sourceConfig.getPattern(), sourceConfig.getVariables()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } catch (InvalidConfigurationException e) {
            meterStatsManager.markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            reportAndThrowError(resultFuture, e);
        }
    }

    protected void reportAndThrowError(ResultFuture<Row> resultFuture, Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) {
        meterStatsManager.markEvent(ExternalSourceAspects.TIMEOUTS);
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
        addMetric(TelemetryTypes.POST_PROCESSOR_TYPE.getValue(), sourceType);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    protected Descriptors.Descriptor getOutputDescriptor(ResultFuture<Row> resultFuture) {
        String descriptorClassName = sourceConfig.getType() != null ? sourceConfig.getType() : schemaConfig.getOutputProtoClassName();
        if (StringUtils.isNotEmpty(descriptorClassName)) {
            try {
                outputDescriptor = descriptorManager.getDescriptor(descriptorClassName);
            } catch (DescriptorNotFoundException descriptorNotFound) {
                reportAndThrowError(resultFuture, descriptorNotFound);
            }
        }
        return outputDescriptor;
    }


}
