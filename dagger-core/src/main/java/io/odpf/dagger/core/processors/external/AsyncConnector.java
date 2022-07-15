package io.odpf.dagger.core.processors.external;

import io.odpf.dagger.core.processors.common.SchemaConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import com.google.protobuf.Descriptors;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.exception.InvalidConfigurationException;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.metrics.reporters.ErrorReporterFactory;
import io.odpf.dagger.core.metrics.telemetry.TelemetryPublisher;
import io.odpf.dagger.core.metrics.telemetry.TelemetryTypes;
import io.odpf.dagger.core.processors.ColumnNameManager;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.common.EndpointHandler;
import io.odpf.dagger.core.processors.types.SourceConfig;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Map;
import java.util.UnknownFormatConversionException;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.singleton;

/**
 * The Async connector.
 */
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

    /**
     * Instantiates a new Async connector.
     *
     * @param sourceType           the source type
     * @param sourceConfig         the source config
     * @param externalMetricConfig the external metric config
     * @param schemaConfig         the schema config
     */
    public AsyncConnector(String sourceType, SourceConfig sourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        this.sourceType = sourceType;
        this.sourceConfig = sourceConfig;
        this.externalMetricConfig = externalMetricConfig;
        this.schemaConfig = schemaConfig;
    }

    /**
     * Gets error reporter.
     *
     * @return the error reporter
     */
    protected ErrorReporter getErrorReporter() {
        return errorReporter;
    }

    /**
     * Gets meter stats manager.
     *
     * @return the meter stats manager
     */
    protected MeterStatsManager getMeterStatsManager() {
        return meterStatsManager;
    }

    /**
     * Gets endpoint handler.
     *
     * @return the endpoint handler
     */
    protected EndpointHandler getEndpointHandler() {
        return endpointHandler;
    }

    /**
     * Gets column name manager.
     *
     * @return the column name manager
     */
    public ColumnNameManager getColumnNameManager() {
        return schemaConfig.getColumnNameManager();
    }

    /**
     * Sets error reporter.
     *
     * @param errorReporter the error reporter
     */
    public void setErrorReporter(ErrorReporter errorReporter) {
        this.errorReporter = errorReporter;
    }

    /**
     * Sets meter stats manager.
     *
     * @param meterStatsManager the meter stats manager
     */
    public void setMeterStatsManager(MeterStatsManager meterStatsManager) {
        this.meterStatsManager = meterStatsManager;
    }

    /**
     * Sets descriptor manager.
     *
     * @param descriptorManager the descriptor manager
     */
    public void setDescriptorManager(DescriptorManager descriptorManager) {
        this.descriptorManager = descriptorManager;
    }

    /**
     * Gets descriptor manager.
     *
     * @return the descriptor manager
     */
    public DescriptorManager getDescriptorManager() {
        return descriptorManager;
    }

    /**
     * Initialize the descriptor manager.
     *
     * @param config the config
     * @return the descriptor manager
     */
    protected DescriptorManager initDescriptorManager(SchemaConfig config) {
        return new DescriptorManager(config.getStencilClientOrchestrator());
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
                    .getErrorReporter(getRuntimeContext().getMetricGroup(), externalMetricConfig.isTelemetryEnabled(), externalMetricConfig.getShutDownPeriod());
        }
        if (meterStatsManager == null) {
            meterStatsManager = new MeterStatsManager(getRuntimeContext().getMetricGroup(), true);
        }
        if (endpointHandler == null) {
            endpointHandler = new EndpointHandler(meterStatsManager, errorReporter,
                    schemaConfig.getInputProtoClasses(), schemaConfig.getColumnNameManager(), descriptorManager);
        }

        String groupKey = TelemetryTypes.SOURCE_METRIC_ID.getValue();
        String groupValue = sourceType + "." + externalMetricConfig.getMetricId();
        meterStatsManager.register(groupKey, groupValue, ExternalSourceAspects.values());
    }

    /**
     * Create client.
     */
    protected abstract void createClient();

    /**
     * Process async.
     *
     * @param input        the input
     * @param resultFuture the result future
     * @throws Exception the exception
     */
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

    /**
     * Report and throw error.
     *
     * @param resultFuture the result future
     * @param exception    the exception
     */
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

    /**
     * Gets output descriptor.
     *
     * @param resultFuture the result future
     * @return the output descriptor
     */
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
