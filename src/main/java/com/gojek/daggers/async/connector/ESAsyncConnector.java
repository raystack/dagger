package com.gojek.daggers.async.connector;

import com.gojek.daggers.Constants;
import com.google.protobuf.Descriptors.Descriptor;
import com.timgroup.statsd.StatsDClient;
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

    private static final String ASPECT = "es.call.count";
    private static StatsDClient statsd;
    private static Descriptor descriptor;
    private RestClient esClient;
    private Integer fieldIndex;
    private Map<String, String> configuration;

    public ESAsyncConnector(StatsDClient statsd, Descriptor descriptor, Integer fieldIndex, Map<String, String> configuration) {
        this.statsd = statsd;
        this.descriptor = descriptor;
        this.fieldIndex = fieldIndex;
        this.configuration = configuration;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (esClient == null) {
            esClient = getEsClient();
        }
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
        statsd.increment(ASPECT);
        EsResponseHandler esResponseHandler = new EsResponseHandler(statsd, input, resultFuture, descriptor, fieldIndex);
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
