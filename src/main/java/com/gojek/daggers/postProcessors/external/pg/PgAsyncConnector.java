package com.gojek.daggers.postProcessors.external.pg;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.exception.InvalidConfigurationException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.AsyncConnector;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.common.PostResponseTelemetry;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolImpl;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.RowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.CLOSE_CONNECTION_ON_EXTERNAL_CLIENT;
import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.INVALID_CONFIGURATION;
import static com.gojek.daggers.utils.Constants.PG_TYPE;

public class PgAsyncConnector extends AsyncConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(PgAsyncConnector.class.getName());
    private final PgSourceConfig pgSourceConfig;
    private PgPool pgClient;

    public PgAsyncConnector(PgSourceConfig pgSourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                            ColumnNameManager columnNameManager, String[] inputProtoClasses, ExternalMetricConfig externalMetricConfig) {
        super(PG_TYPE, pgSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
        this.pgSourceConfig = pgSourceConfig;
    }

    public PgAsyncConnector(PgSourceConfig pgSourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                            ColumnNameManager columnNameManager, MeterStatsManager meterStatsManager, PgPool pgClient,
                            ErrorReporter errorReporter, ExternalMetricConfig externalMetricConfig, String[] inputProtoClasses) {
        this(pgSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
        this.pgClient = pgClient;
        setErrorReporter(errorReporter);
        setMeterStatsManager(meterStatsManager);
    }

    @Override
    protected void createClient() {
        if (pgClient == null) {
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

            pgClient = pool(connectOptions, poolOptions);
        }
    }

    @Override
    public void process(Row input, ResultFuture<Row> resultFuture) {
        RowManager rowManager = new RowManager(input);

        Object[] queryVariablesValues = getEndpointHandler()
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);
        if (getEndpointHandler().isEndpointOrQueryInvalid(resultFuture, rowManager, queryVariablesValues)) {
            return;
        }

        String query = String.format(pgSourceConfig.getPattern(), queryVariablesValues);
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, getMeterStatsManager(), rowManager,
                getColumnNameManager(), getOutputDescriptor(resultFuture), resultFuture, getErrorReporter(), new PostResponseTelemetry());

        pgResponseHandler.startTimer();
        Query<RowSet<io.vertx.sqlclient.Row>> executableQuery = pgClient.query(query);
        if (executableQuery == null) {
            getMeterStatsManager().markEvent(INVALID_CONFIGURATION);
            Exception invalidConfigurationException = new InvalidConfigurationException(String.format("Query '%s' is invalid", query));
            reportAndThrowError(resultFuture, invalidConfigurationException);
        } else {
            executableQuery.execute(pgResponseHandler);
        }
    }

    @Override
    public void close() {
        pgClient.close();
        pgClient = null;
        getMeterStatsManager().markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
        LOGGER.info("DB Connector : Connection pool released");
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

    Object getPgCient() {
        return pgClient;
    }
}
