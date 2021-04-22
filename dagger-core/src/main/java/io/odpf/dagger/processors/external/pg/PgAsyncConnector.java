package io.odpf.dagger.processors.external.pg;

import io.odpf.dagger.exception.InvalidConfigurationException;
import io.odpf.dagger.metrics.MeterStatsManager;
import io.odpf.dagger.metrics.reporters.ErrorReporter;
import io.odpf.dagger.processors.external.AsyncConnector;
import io.odpf.dagger.processors.external.ExternalMetricConfig;
import io.odpf.dagger.processors.external.SchemaConfig;
import io.odpf.dagger.processors.common.PostResponseTelemetry;
import io.odpf.dagger.processors.common.RowManager;
import io.odpf.dagger.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.utils.Constants;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolImpl;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.RowSet;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PgAsyncConnector extends AsyncConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(PgAsyncConnector.class.getName());
    private final PgSourceConfig pgSourceConfig;
    private PgPool pgClient;

    public PgAsyncConnector(PgSourceConfig pgSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig,
                            MeterStatsManager meterStatsManager, PgPool pgClient, ErrorReporter errorReporter) {
        this(pgSourceConfig, externalMetricConfig, schemaConfig);
        this.pgClient = pgClient;
        setErrorReporter(errorReporter);
        setMeterStatsManager(meterStatsManager);
    }

    public PgAsyncConnector(PgSourceConfig pgSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        super(Constants.PG_TYPE, pgSourceConfig, externalMetricConfig, schemaConfig);
        this.pgSourceConfig = pgSourceConfig;
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
        if (getEndpointHandler().isQueryInvalid(resultFuture, rowManager, queryVariablesValues)) {
            return;
        }

        String query = String.format(pgSourceConfig.getPattern(), queryVariablesValues);
        PgResponseHandler pgResponseHandler = new PgResponseHandler(pgSourceConfig, getMeterStatsManager(), rowManager,
                getColumnNameManager(), getOutputDescriptor(resultFuture), resultFuture, getErrorReporter(), new PostResponseTelemetry());

        pgResponseHandler.startTimer();
        Query<RowSet<io.vertx.sqlclient.Row>> executableQuery = pgClient.query(query);
        if (executableQuery == null) {
            getMeterStatsManager().markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
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
        getMeterStatsManager().markEvent(ExternalSourceAspects.CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
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