package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.DescriptorNotFoundException;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.metrics.reporters.ErrorReporterFactory;
import com.gojek.daggers.metrics.telemetry.TelemetryPublisher;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.external.common.SourceConfig;
import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.SOURCE_METRIC_ID;
import static java.util.Collections.singleton;

public abstract class AsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {
    private StencilClientOrchestrator stencilClientOrchestrator;
    private StencilClient stencilClient;
    private String metricId;
    private ErrorReporter errorReporter;
    private MeterStatsManager meterStatsManager;
    private ColumnNameManager columnNameManager;
    private Boolean telemetryEnabled;
    private long shutDownPeriod;
    private SourceConfig sourceConfig;
    private Map<String, List<String>> metrics = new HashMap<>();
    private String sourceType;
    private Descriptors.Descriptor outputDescriptor;

    public AsyncConnector(String sourceType, SourceConfig sourceConfig, String metricId, StencilClientOrchestrator stencilClientOrchestrator,
                          ColumnNameManager columnNameManager, boolean telemetryEnabled, long shutDownPeriod) {
        this.sourceType = sourceType;
        this.sourceConfig = sourceConfig;
        this.metricId = metricId;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
    }

    protected ErrorReporter getErrorReporter() {
        return errorReporter;
    }

    protected MeterStatsManager getMeterStatsManager() {
        return meterStatsManager;
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

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (stencilClient == null) {
            stencilClient = stencilClientOrchestrator.getStencilClient();
        }

        createClient();

        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext(), telemetryEnabled, shutDownPeriod);
        }
        if (meterStatsManager == null)
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);

        String groupKey = SOURCE_METRIC_ID.getValue();
        String groupValue = sourceType + "." + metricId;
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

    protected Object[] getEndpointOrQueryVariablesValues(RowManager rowManager) {
        String queryVariables = sourceConfig.getVariables();
        if (StringUtils.isEmpty(queryVariables)) return new Object[0];

        List<String> requiredInputColumns = Arrays.asList(queryVariables.split(","));
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        for (String inputColumnName : requiredInputColumns) {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            if (inputColumnIndex == -1) {
                throw new InvalidConfigurationException(String.format("Column '%s' not found as configured in the endpoint/query variable", inputColumnName));
            }
            inputColumnValues.add(rowManager.getFromInput(inputColumnIndex));
        }
        return inputColumnValues.toArray();
    }

    protected boolean isEndpointOrQueryInvalid(ResultFuture<Row> resultFuture, RowManager rowManager, Object[] endpointVariablesValues) {
        boolean emptyEndpointPattern = StringUtils.isEmpty(sourceConfig.getPattern());
        if (emptyEndpointPattern) {
            meterStatsManager.markEvent(ExternalSourceAspects.EMPTY_INPUT);
            resultFuture.complete(singleton(rowManager.getAll()));
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Endpoint/Query Pattern '%s' is invalid, Can't be empty. ", sourceConfig.getPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
            return true;
        }
        if (!StringUtils.isEmpty(sourceConfig.getVariables()) && Arrays.asList(endpointVariablesValues).isEmpty()) {
            meterStatsManager.markEvent(ExternalSourceAspects.EMPTY_INPUT);
            resultFuture.complete(singleton(rowManager.getAll()));
            return true;
        }
        return false;
    }

    protected Descriptors.Descriptor getOutputDescriptor(ResultFuture<Row> resultFuture) {
        if (sourceConfig.getType() != null) {
            String descriptorType = sourceConfig.getType();
            outputDescriptor = stencilClient.get(descriptorType);
            if (outputDescriptor == null) {
                reportAndThrowError(resultFuture, new DescriptorNotFoundException("No Descriptor found for class " + descriptorType));
            }
        }
        return outputDescriptor;
    }
}
