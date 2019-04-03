package com.gojek.daggers.async.connector;

import com.gojek.daggers.async.connector.metric.Aspects;
import com.gojek.daggers.async.connector.metric.StatsManager;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.util.Map;

import static com.gojek.daggers.Constants.*;


public class ESAsyncConnector extends RichAsyncFunction<Row, Row> {

    private Descriptors.Descriptor descriptor;
    private RestClient esClient;
    private Integer fieldIndex;
    private Map<String, String> configuration;
    private StencilClient stencilClient;
    private StatsManager statsManager;

    public ESAsyncConnector(Integer fieldIndex, Map<String, String> configuration, StencilClient stencilClient) {
        this.fieldIndex = fieldIndex;
        this.configuration = configuration;
        this.stencilClient = stencilClient;

    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        String descriptorType = this.configuration.get("type");
        this.descriptor = stencilClient.get(descriptorType);

        if (esClient == null) {
            esClient = getEsClient();
        }
        statsManager = new StatsManager(getRuntimeContext(), true);
        statsManager.register();
    }

    protected RestClient getEsClient() {
        Integer connectTimeout = getIntegerConfig(configuration, ASYNC_IO_ES_CONNECT_TIMEOUT_KEY);
        Integer socketTimeout = getIntegerConfig(configuration, ASYNC_IO_ES_SOCKET_TIMEOUT_KEY);
        Integer retryTimeout = getIntegerConfig(configuration, ASYNC_IO_ES_MAX_RETRY_TIMEOUT_KEY);
        return RestClient.builder(
                getHttpHosts(getEsHost())
        ).setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                        .setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout))
                .setMaxRetryTimeoutMillis(retryTimeout).build();
    }

    private HttpHost[] getHttpHosts(String[] hosts) {
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < httpHosts.length; i++) {
            String[] strings = getEsHost()[i].split(":");
            httpHosts[i] = new HttpHost(strings[0], Integer.parseInt(strings[1]));
        }
        return httpHosts;
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        Object id = ((Row) input.getField(0)).getField(getIntegerConfig(configuration, ASYNC_IO_ES_INPUT_INDEX_KEY));
        String esEndpoint = String.format(configuration.get(ASYNC_IO_ES_PATH_KEY), id);
        Request request = new Request("GET", esEndpoint);
        statsManager.markEvent(Aspects.TOTAL_CALLS);
        EsResponseHandler esResponseHandler = new EsResponseHandler(input, resultFuture, descriptor, fieldIndex, statsManager);
        esResponseHandler.start();
        esClient.performRequestAsync(request, esResponseHandler);
    }

    private Integer getIntegerConfig(Map<String, String> fieldConfiguration, String key) {
        return Integer.valueOf(fieldConfiguration.get(key));
    }

    private String[] getEsHost() {
        String host = configuration.get(ASYNC_IO_ES_HOST_KEY);
        return host.split(",");
    }
}
