package com.gojek.daggers.postProcessors.external.pg;

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
import com.gojek.de.stencil.client.StencilClient;
import com.google.protobuf.Descriptors;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.utils.Constants.ASHIKO_PG_PROCESSOR;
import static java.util.Collections.singleton;

public class PgAsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {
    private final PgSourceConfig pgSourceConfig;
    private final StencilClientOrchestrator stencilClientOrchestrator;
    private final ColumnNameManager columnNameManager;
    private final boolean telemetryEnabled;
    private final long shutDownPeriod;
    private StencilClient stencilClient;
    private PgPool pgClient;
    private MeterStatsManager meterStatsManager;
    private ErrorReporter errorReporter;
    private HashMap<String, List<String>> metrics = new HashMap<>();
    private Descriptors.Descriptor outputDescriptor;

    public PgAsyncConnector(PgSourceConfig pgSourceConfig, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager, boolean telemetryEnabled, long shutDownPeriod) {

        this.pgSourceConfig = pgSourceConfig;
        this.stencilClientOrchestrator = stencilClientOrchestrator;
        this.columnNameManager = columnNameManager;
        this.telemetryEnabled = telemetryEnabled;
        this.shutDownPeriod = shutDownPeriod;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (stencilClient == null) {
            stencilClient = stencilClientOrchestrator.getStencilClient();
        }
        if (pgClient == null) {
            pgClient = createPgClient();
        }
        if (errorReporter == null) {
            errorReporter = ErrorReporterFactory.getErrorReporter(getRuntimeContext(), telemetryEnabled, shutDownPeriod);
        }
        List<String> pgOutputColumnNames = pgSourceConfig.getOutputColumns();
        pgOutputColumnNames.forEach(outputField -> {
            String groupName = "pg." + pgOutputColumnNames;
            meterStatsManager = new MeterStatsManager(getRuntimeContext(), true);
            meterStatsManager.register(groupName, values());
        });
    }

    private PgPool createPgClient() {
        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(pgSourceConfig.getPort())
                .setHost(pgSourceConfig.getHost())
                .setDatabase(pgSourceConfig.getDatabase())
                .setUser(pgSourceConfig.getUser())
                .setPassword(pgSourceConfig.getPassword())
                .setConnectTimeout(pgSourceConfig.getConnectTimeout())
                .setIdleTimeout(pgSourceConfig.getIdleTimeout());

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(pgSourceConfig.getMaximumConnectionPoolSize());

        return PgPool.pool(connectOptions, poolOptions);
    }

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(POST_PROCESSOR_TYPE.getValue(), ASHIKO_PG_PROCESSOR);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

        RowManager rowManager = new RowManager(input);
        Object[] queryVariablesValues = getQueryVariablesValues(rowManager, resultFuture);
        if (isQueryInvalid(resultFuture, rowManager, queryVariablesValues)) return;
        String query = String.format(pgSourceConfig.getQueryPattern(), queryVariablesValues);
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager,
                columnNameManager, getOutputDescriptor(resultFuture), resultFuture, errorReporter);
        pgResponseHandler.startTimer();
        pgClient.query(query).execute(pgResponseHandler::handle);
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) {
        meterStatsManager.markEvent(TIMEOUTS);
        Exception timeoutException = new TimeoutException("Timeout in Postgres Call");
        errorReporter.reportNonFatalException(timeoutException);
        resultFuture.complete(singleton(input));
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    private Object[] getQueryVariablesValues(RowManager rowManager, ResultFuture<Row> resultFuture) {
        List<String> requiredInputColumns = Arrays.asList(pgSourceConfig.getQueryVariables().split(","));
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        for (String inputColumnName : requiredInputColumns) {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            if (inputColumnIndex == -1) {
                meterStatsManager.markEvent(INVALID_CONFIGURATION);
                Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Column '%s' not found as configured in the query variable", inputColumnName));
                reportAndThrowError(resultFuture, invalidConfigurationException);
                return new Object[0];
            }
            Object inputColumnValue = rowManager.getFromInput(inputColumnIndex);
            if (inputColumnValue != null && StringUtils.isNotEmpty(String.valueOf(inputColumnValue)))
                inputColumnValues.add(inputColumnValue);
        }

        return inputColumnValues.toArray();
    }

    private Descriptors.Descriptor   getOutputDescriptor(ResultFuture<Row> resultFuture) {
        if (pgSourceConfig.hasType()) {
            String descriptorType = pgSourceConfig.getType();
            outputDescriptor = stencilClient.get(descriptorType);
            if (outputDescriptor == null) {
                reportAndThrowError(resultFuture, new DescriptorNotFoundException("No Descriptor found for class " + descriptorType));
            }
        }
        return outputDescriptor;
    }

    private boolean isQueryInvalid(ResultFuture<Row> resultFuture, RowManager rowManager, Object[]
            queryVariablesValues) {
        boolean invalidQueryPattern = StringUtils.isEmpty(pgSourceConfig.getQueryPattern());
        boolean emptyQueryVariable = Arrays.asList(queryVariablesValues).isEmpty();
        if (invalidQueryPattern || emptyQueryVariable) {
            meterStatsManager.markEvent(ExternalSourceAspects.EMPTY_INPUT);
            resultFuture.complete(singleton(rowManager.getAll()));
            return true;
        }
        return false;
    }

    private void reportAndThrowError(ResultFuture<Row> resultFuture, Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }
}
