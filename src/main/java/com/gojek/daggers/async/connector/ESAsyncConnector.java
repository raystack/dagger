package com.gojek.daggers.async.connector;

import com.timgroup.statsd.StatsDClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;


public class ESAsyncConnector extends RichAsyncFunction<Row, Tuple2<String, String>> {

    private static final String ASPECT = "es.call.count";

    private static StatsDClient statsd;
    private final Integer connectTimeout;
    private final Integer socketTimeout;
    private final int maxRetryTimeout;
    private String[] host;
    private RestClient esClient;

    public ESAsyncConnector(String esHosts, int connectTimeout, int socketTimeout, int maxRetryTimeout, StatsDClient statsd) {
        this.statsd = statsd;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
        this.maxRetryTimeout = maxRetryTimeout;
        this.host = esHosts.split(",");
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        if (esClient == null) {
            esClient = getEsClient();
        }
    }

    protected RestClient getEsClient() {
        return RestClient.builder(
                getHttpHosts(host)
        ).setRequestConfigCallback(requestConfigBuilder ->
                requestConfigBuilder
                        .setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout))
                .setMaxRetryTimeoutMillis(maxRetryTimeout).build();
    }

    private HttpHost[] getHttpHosts(String[] hosts) {
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        for (int i = 0; i < httpHosts.length; i++) {
            String[] strings = host[i].split(":");
            httpHosts[i] = new HttpHost(strings[0], Integer.parseInt(strings[1]));
        }
        return httpHosts;
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Tuple2<String, String>> resultFuture) {
        String customerId = (String) input.getField(5);
        String esEndpoint = String.format("/customers/customer/%s", customerId);
        Request request = new Request("GET", esEndpoint);
        statsd.increment(ASPECT);
        EsResponseHandler esResponseHandler = new EsResponseHandler(statsd, input, resultFuture, customerId);
        esResponseHandler.start();
        esClient.performRequestAsync(request, esResponseHandler);
    }
}
