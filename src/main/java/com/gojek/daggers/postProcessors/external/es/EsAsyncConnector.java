package com.gojek.daggers.postProcessors.external.es;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.AsyncConnector;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.common.PostResponseTelemetry;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.gojek.daggers.utils.Constants.ES_TYPE;


public class EsAsyncConnector extends AsyncConnector {

    private RestClient esClient;
    private EsSourceConfig esSourceConfig;

    public EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClientOrchestrator stencilClientOrchestrator,
                            ColumnNameManager columnNameManager, String[] inputProtoClasses, ExternalMetricConfig externalMetricConfig) {
        super(ES_TYPE, esSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
        this.esSourceConfig = esSourceConfig;
    }

    EsAsyncConnector(EsSourceConfig esSourceConfig, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager,
                     RestClient esClient, ErrorReporter errorReporter, MeterStatsManager meterStatsManager, ExternalMetricConfig externalMetricConfig,
                     String[] inputProtoClasses) {
        this(esSourceConfig, stencilClientOrchestrator, columnNameManager, inputProtoClasses, externalMetricConfig);
        this.esClient = esClient;
        setErrorReporter(errorReporter);
        setMeterStatsManager(meterStatsManager);
    }

    @Override
    protected void createClient() {
        if (esClient == null) {
            esClient = RestClient.builder(
                    getHttpHosts()
            ).setRequestConfigCallback(requestConfigBuilder ->
                    requestConfigBuilder
                            .setConnectTimeout(esSourceConfig.getConnectTimeout())
                            .setSocketTimeout(esSourceConfig.getSocketTimeout()))
                    .setMaxRetryTimeoutMillis(esSourceConfig.getRetryTimeout()).build();
        }
    }

    @Override
    protected void process(Row input, ResultFuture<Row> resultFuture) {
        RowManager rowManager = new RowManager(input);
        Object[] endpointVariablesValues = getEndpointHandler()
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);
        if (getEndpointHandler().isEndpointOrQueryInvalid(resultFuture, rowManager, endpointVariablesValues)) {
            return;
        }
        String esEndpoint = String.format(esSourceConfig.getPattern(), endpointVariablesValues);
        Request esRequest = new Request("GET", esEndpoint);
        EsResponseHandler esResponseHandler = new EsResponseHandler(esSourceConfig, getMeterStatsManager(), rowManager,
                getColumnNameManager(), getOutputDescriptor(resultFuture), resultFuture, getErrorReporter(), new PostResponseTelemetry());
        esResponseHandler.startTimer();
        esClient.performRequestAsync(esRequest, esResponseHandler);
    }

    private HttpHost[] getHttpHosts() {
        List<String> hosts = Arrays.asList(esSourceConfig.getHost().split(","));
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        hosts.forEach(s -> httpHosts.add(new HttpHost(s, esSourceConfig.getPort())));
        return httpHosts.toArray(new HttpHost[0]);
    }
}
