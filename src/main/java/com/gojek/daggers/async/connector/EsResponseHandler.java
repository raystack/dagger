package com.gojek.daggers.async.connector;

import com.timgroup.statsd.StatsDClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.http.client.ResponseHandler;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;

import java.io.IOException;
import java.util.Collections;

public class EsResponseHandler implements ResponseListener {
    private static final String ASPECT = "es.call.count";
    private static final String SUCCESS_ASPECT = "es.success.count";
    private static final String EXCEPTION_ASPECT = "es.failed.count";
    private static final String RESPONSE_ASPECT = "es.response.time";
    private static final String FOUR_XX_ASPECT = "es.4XX.failed.count";
    private static final String FIVE_XX_ASPECT = "es.5XX.failed.count";
    private StatsDClient statsDClient;
    private Row input;
    private ResultFuture<Tuple2<String, String>> resultFuture;
    private String customerId;
    private long startTime;

    public EsResponseHandler(StatsDClient statsDClient, Row input, ResultFuture<Tuple2<String, String>> resultFuture, String customerId) {
        this.statsDClient = statsDClient;
        this.input = input;
        this.resultFuture = resultFuture;
        this.customerId = customerId;
    }

    public void start(){
        startTime = System.nanoTime();
    }

    @Override
    public void onSuccess(Response response) {
        if (response.getStatusLine().getStatusCode() == 200) {
            statsDClient.increment(SUCCESS_ASPECT);
            try {
                String responseBody = EntityUtils.toString(response.getEntity());
                resultFuture.complete(Collections.singleton(new Tuple2<>(customerId, input.toString().concat("||" + responseBody))));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            statsDClient.increment(FOUR_XX_ASPECT);
            System.err.println("ElasticSearch Service 4XX Error : Code : 404");
            resultFuture.complete(Collections.singleton(new Tuple2<>(customerId, input.toString())));
        }
        statsDClient.time(RESPONSE_ASPECT, getElapsedTimeInMillis(startTime));
    }

    @Override
    public void onFailure(Exception e) {
        if (e instanceof ResponseException) {
            statsDClient.increment(FOUR_XX_ASPECT);
            System.err.println("ElasticSearch Service 4XX Error : Code : 4XX");
        } else {
            statsDClient.increment(FIVE_XX_ASPECT);
            System.err.println("ElasticSearch Service 5XX Error : Code : 5XX");
        }
        e.printStackTrace();
        statsDClient.increment(EXCEPTION_ASPECT);
        statsDClient.time(RESPONSE_ASPECT, getElapsedTimeInMillis(startTime));
        resultFuture.complete(Collections.singleton(new Tuple2<>(customerId, input.toString())));
    }

    private long getElapsedTimeInMillis(long startTime) {
        return (System.nanoTime() - startTime) / 1000000;
    }
}
