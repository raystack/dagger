package io.odpf.dagger.core.processors.external.http;

import io.odpf.dagger.core.exception.InvalidHttpVerbException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.common.PostResponseTelemetry;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.external.AsyncConnector;
import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.common.SchemaConfig;
import io.odpf.dagger.core.processors.external.http.request.HttpRequestFactory;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import io.odpf.dagger.core.utils.Constants;
import io.odpf.dagger.core.utils.Constants.ExternalPostProcessorVariableType;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

/**
 * The Http async connector.
 */
public class HttpAsyncConnector extends AsyncConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAsyncConnector.class.getName());
    private AsyncHttpClient httpClient;
    private HttpSourceConfig httpSourceConfig;

    /**
     * Instantiates a new Http async connector with specified http client.
     *
     * @param httpSourceConfig     the http source config
     * @param externalMetricConfig the external metric config
     * @param schemaConfig         the schema config
     * @param httpClient           the http client
     * @param errorReporter        the error reporter
     * @param meterStatsManager    the meter stats manager
     * @param descriptorManager    the descriptor manager
     */
    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig,
                              AsyncHttpClient httpClient, ErrorReporter errorReporter, MeterStatsManager meterStatsManager, DescriptorManager descriptorManager) {
        this(httpSourceConfig, externalMetricConfig, schemaConfig);
        this.httpClient = httpClient;
        setErrorReporter(errorReporter);
        setMeterStatsManager(meterStatsManager);
        setDescriptorManager(descriptorManager);
    }

    /**
     * Instantiates a new Http async connector.
     *
     * @param httpSourceConfig     the http source config
     * @param externalMetricConfig the external metric config
     * @param schemaConfig         the schema config
     */
    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        super(Constants.HTTP_TYPE, httpSourceConfig, externalMetricConfig, schemaConfig);
        this.httpSourceConfig = httpSourceConfig;
    }

    /**
     * Gets http client.
     *
     * @return the http client
     */
    AsyncHttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    protected void createClient() {
        if (httpClient == null) {
            httpClient = asyncHttpClient(config().setConnectTimeout(httpSourceConfig.getConnectTimeout()));
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
        httpClient = null;
        getMeterStatsManager().markEvent(ExternalSourceAspects.CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
        LOGGER.error("HTTP Connector : Connection closed");
    }

    @Override
    protected void process(Row input, ResultFuture<Row> resultFuture) {
        try {
            RowManager rowManager = new RowManager(input);

            Object[] requestVariablesValues = getEndpointHandler()
                    .getVariablesValue(rowManager, ExternalPostProcessorVariableType.REQUEST_VARIABLES, httpSourceConfig.getRequestVariables(), resultFuture);
            Object[] dynamicHeaderVariablesValues = getEndpointHandler()
                    .getVariablesValue(rowManager, ExternalPostProcessorVariableType.HEADER_VARIABLES, httpSourceConfig.getHeaderVariables(), resultFuture);
            if (getEndpointHandler().isQueryInvalid(resultFuture, rowManager, httpSourceConfig.getRequestVariables(), requestVariablesValues) || getEndpointHandler().isQueryInvalid(resultFuture, rowManager, httpSourceConfig.getHeaderVariables(), dynamicHeaderVariablesValues)) {
                return;
            }

            BoundRequestBuilder request = HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues, dynamicHeaderVariablesValues);
            HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, getMeterStatsManager(),
                    rowManager, getColumnNameManager(), getOutputDescriptor(resultFuture), resultFuture, getErrorReporter(), new PostResponseTelemetry());
            httpResponseHandler.startTimer();
            request.execute(httpResponseHandler);
        } catch (InvalidHttpVerbException e) {
            getMeterStatsManager().markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            resultFuture.completeExceptionally(e);
        }

    }
}
