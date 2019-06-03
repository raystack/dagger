package com.gojek.daggers.async.connector;

import com.gojek.daggers.async.builder.ResponseBuilder;
import com.gojek.daggers.async.metric.AsyncAspects;
import com.gojek.daggers.utils.stats.StatsManager;
import com.gojek.de.stencil.StencilClient;
import com.google.protobuf.Descriptors;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.util.Map;
import java.util.Objects;

import static com.gojek.daggers.Constants.*;
import static java.util.Collections.singleton;


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

    ESAsyncConnector(Integer fieldIndex, Map<String, String> configuration, StencilClient stencilClient, RestClient esClient) {
        this.fieldIndex = fieldIndex;
        this.configuration = configuration;
        this.stencilClient = stencilClient;
        this.esClient = esClient;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        String descriptorType = this.configuration.get("type");
        this.descriptor = stencilClient.get(descriptorType);

        if (esClient == null) {
            esClient = createESClient();
        }
        String groupName = "es." + this.configuration.get(FIELD_NAME_KEY);
        statsManager = new StatsManager(getRuntimeContext(), true);
        statsManager.register(groupName, AsyncAspects.values());
    }

    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {
        statsManager.markEvent(AsyncAspects.TIMEOUTS);
        resultFuture.complete(singleton(new ResponseBuilder(input).build()));
    }

    private RestClient createESClient() {
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
            httpHosts[i] = new HttpHost(strings[0].trim(), Integer.parseInt(strings[1].trim()));
        }
        return httpHosts;
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        Object id = ((Row) input.getField(0)).getField(getIntegerConfig(configuration, ASYNC_IO_ES_INPUT_INDEX_KEY));
        if (isEmpty(id)) {
            resultFuture.complete(singleton(new ResponseBuilder(input).build()));
            statsManager.markEvent(AsyncAspects.EMPTY_INPUT);
            return;
        }
        String esEndpoint = String.format(configuration.get(ASYNC_IO_ES_PATH_KEY), id);
        Request request = new Request("GET", esEndpoint);
        statsManager.markEvent(AsyncAspects.TOTAL_ES_CALLS);
        EsResponseHandler esResponseHandler = new EsResponseHandler(input, resultFuture, descriptor, fieldIndex, statsManager);
        esResponseHandler.start();
        esClient.performRequestAsync(request, esResponseHandler);
    }

    private boolean isEmpty(Object value) {
        return Objects.isNull(value) || StringUtils.isEmpty(value.toString());
    }

    private Integer getIntegerConfig(Map<String, String> fieldConfiguration, String key) {
        return Integer.valueOf(fieldConfiguration.get(key));
    }

    private String[] getEsHost() {
        String host = configuration.get(ASYNC_IO_ES_HOST_KEY);
        return host.split(",");
    }
}
