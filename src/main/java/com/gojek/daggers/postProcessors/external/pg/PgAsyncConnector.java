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
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolImpl;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.RowSet;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static com.gojek.daggers.metrics.telemetry.TelemetryTypes.POST_PROCESSOR_TYPE;
import static com.gojek.daggers.utils.Constants.ASHIKO_PG_PROCESSOR;
import static java.util.Collections.singleton;

public class PgAsyncConnector extends RichAsyncFunction<Row, Row> implements TelemetryPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(PgAsyncConnector.class.getName());
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

    public PgAsyncConnector(PgSourceConfig pgSourceConfig, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager, MeterStatsManager meterStatsManager, PgPool pgClient, boolean telemetryEnabled, ErrorReporter errorReporter, long shutDownPeriod, StencilClient stencilClient) {
        this(pgSourceConfig, stencilClientOrchestrator, columnNameManager, telemetryEnabled, shutDownPeriod);
        this.meterStatsManager = meterStatsManager;
        this.pgClient = pgClient;
        this.errorReporter = errorReporter;
        this.stencilClient = stencilClient;
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

    @Override
    public Map<String, List<String>> getTelemetry() {
        return metrics;
    }

    @Override
    public void preProcessBeforeNotifyingSubscriber() {
        addMetric(POST_PROCESSOR_TYPE.getValue(), ASHIKO_PG_PROCESSOR);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        try {
            RowManager rowManager = new RowManager(input);
            Object[] queryVariablesValues = getQueryVariablesValues(rowManager);
            if (isQueryInvalid(resultFuture, queryVariablesValues)) return;
            String query = String.format(pgSourceConfig.getQueryPattern(), queryVariablesValues);
            PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, meterStatsManager, rowManager,
                    columnNameManager, getOutputDescriptor(resultFuture), resultFuture, errorReporter);

            pgResponseHandler.startTimer();
            Query<RowSet<io.vertx.sqlclient.Row>> executableQuery = pgClient.query(query);
            if (executableQuery == null) {
                meterStatsManager.markEvent(INVALID_CONFIGURATION);
                Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Query '%s' is invalid", query));
                reportAndThrowError(resultFuture, invalidConfigurationException);
            } else {
                executableQuery.execute(pgResponseHandler);
            }
        } catch (UnknownFormatConversionException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Query pattern '%s' is invalid", pgSourceConfig.getQueryPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } catch (IllegalFormatException e) {
            meterStatsManager.markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Query pattern '%s' is incompatible with the query variables", pgSourceConfig.getQueryPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        }
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) {
        meterStatsManager.markEvent(TIMEOUTS);
        Exception timeoutException = new TimeoutException("Timeout in DB Call");
        if (pgSourceConfig.isFailOnErrors()) {
            reportAndThrowError(resultFuture, timeoutException);
        } else {
            errorReporter.reportNonFatalException(timeoutException);
        }
        resultFuture.complete(singleton(input));
    }

    @Override
    public void close() {
        pgClient.close();
        pgClient = null;
        meterStatsManager.markEvent(CLOSE_CONNECTION_ON_DB_CLIENT);
        LOGGER.info("DB Connector : Connection pool released");
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
                .setMaxSize(pgSourceConfig.getCapacity());

        return pool(connectOptions, poolOptions);
    }

    private PgPool pool(PgConnectOptions connectOptions, PoolOptions poolOptions) {
        if (Vertx.currentContext() != null) {
            throw new IllegalStateException("Running in a Vertx context => use PgPool#pool(Vertx, PgConnectOptions, PoolOptions) instead");
        }
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setMaxEventLoopExecuteTime(10000);
        vertxOptions.setMaxEventLoopExecuteTimeUnit(TimeUnit.MILLISECONDS);
        if (connectOptions.isUsingDomainSocket()) {
            vertxOptions.setPreferNativeTransport(true);
        }
        Vertx vertx = Vertx.vertx(vertxOptions);
        return new PgPoolImpl(vertx.getOrCreateContext(), true, connectOptions, poolOptions);
    }

    private void addMetric(String key, String value) {
        metrics.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
    }

    private Object[] getQueryVariablesValues(RowManager rowManager) {
        String queryVariables = pgSourceConfig.getQueryVariables();
        if (queryVariables == null) queryVariables = "";
        List<String> requiredInputColumns = Arrays.asList(queryVariables.split(","));
        ArrayList<Object> inputColumnValues = new ArrayList<>();
        for (String inputColumnName : requiredInputColumns) {
            int inputColumnIndex = columnNameManager.getInputIndex(inputColumnName);
            if (inputColumnIndex == -1) {
                Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Column '%s' not found as configured in the query variable", inputColumnName));
                if (pgSourceConfig.getQueryPattern().matches(".*=\\s*'%.*")) {
                    meterStatsManager.markEvent(INVALID_CONFIGURATION);
                    errorReporter.reportFatalException(invalidConfigurationException);
                }
                return new Object[0];
            }
            Object inputColumnValue = rowManager.getFromInput(inputColumnIndex);
            if (inputColumnValue != null && StringUtils.isNotEmpty(String.valueOf(inputColumnValue)))
                inputColumnValues.add(inputColumnValue);
        }

        return inputColumnValues.toArray();
    }

    private Descriptors.Descriptor getOutputDescriptor(ResultFuture<Row> resultFuture) {
        if (pgSourceConfig.hasType()) {
            String descriptorType = pgSourceConfig.getType();
            outputDescriptor = stencilClient.get(descriptorType);
            if (outputDescriptor == null) {
                reportAndThrowError(resultFuture, new DescriptorNotFoundException("No Descriptor found for class " + descriptorType));
            }
        }
        return outputDescriptor;
    }

    private boolean isQueryInvalid(ResultFuture<Row> resultFuture, Object[] queryVariablesValues) {
        boolean emptyQueryPattern = StringUtils.isEmpty(pgSourceConfig.getQueryPattern());
        boolean emptyQueryVariableValue = Arrays.asList(queryVariablesValues).isEmpty();
        if ((emptyQueryPattern)) {
            meterStatsManager.markEvent(ExternalSourceAspects.EMPTY_INPUT);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Query Pattern '%s' is Invalid.", pgSourceConfig.getQueryPattern()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
            return true;
        } else if (pgSourceConfig.getQueryPattern().matches(".*=\\s*'%.*") && emptyQueryVariableValue) {
            meterStatsManager.markEvent(ExternalSourceAspects.EMPTY_INPUT);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Query Variables '%s' have Invalid or No values", pgSourceConfig.getQueryVariables()));
            reportAndThrowError(resultFuture, invalidConfigurationException);
            return true;
        }
        return false;
    }

    private void reportAndThrowError(ResultFuture<Row> resultFuture, Exception exception) {
        errorReporter.reportFatalException(exception);
        resultFuture.completeExceptionally(exception);
    }

    Object getPgCient() {
        return pgClient;
    }
}
