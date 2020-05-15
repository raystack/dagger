package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.core.StencilClientOrchestrator;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.AsyncConnector;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
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

    public EsAsyncConnector(EsSourceConfig esSourceConfig, String metricId, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager,
                            boolean telemetryEnabled, long shutDownPeriod) {
        super(ES_TYPE, esSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, telemetryEnabled, shutDownPeriod);
        this.esSourceConfig = esSourceConfig;
    }

    EsAsyncConnector(EsSourceConfig esSourceConfig, String metricId, StencilClientOrchestrator stencilClientOrchestrator, ColumnNameManager columnNameManager,
                     RestClient esClient, boolean telemetryEnabled, long shutDownPeriod) {
        this(esSourceConfig, metricId, stencilClientOrchestrator, columnNameManager, telemetryEnabled, shutDownPeriod);
        this.esClient = esClient;
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
        Object[] endpointVariablesValues = getEndpointOrQueryVariablesValues(rowManager, resultFuture);
        if (isEndpointOrQueryInvalid(resultFuture, rowManager)) return;
        String esEndpoint = String.format(esSourceConfig.getPattern(), endpointVariablesValues);
        Request esRequest = new Request("GET", esEndpoint);
        EsResponseHandler esResponseHandler = new EsResponseHandler(esSourceConfig, getMeterStatsManager(), rowManager,
                columnNameManager, getOutputDescriptor(resultFuture), resultFuture, getErrorReporter());
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
