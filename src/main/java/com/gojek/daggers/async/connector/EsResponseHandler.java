package com.gojek.daggers.async.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.daggers.async.connector.metric.StatsManager;
import com.gojek.daggers.builder.ResponseBuilder;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;

import java.io.IOException;
import java.util.Map;

import static com.gojek.daggers.async.connector.metric.Aspects.*;
import static com.gojek.daggers.utils.RowMaker.makeRow;
import static java.util.Collections.singleton;

public class EsResponseHandler implements ResponseListener {
    private Row input;
    private ResultFuture<Row> resultFuture;
    private long startTime;
    private Descriptor descriptor;
    private Integer fieldIndex;
    private StatsManager statsManager;


    public EsResponseHandler(Row input, ResultFuture<Row> resultFuture, Descriptor descriptor, Integer fieldIndex, StatsManager statsManager) {
        this.input = input;
        this.resultFuture = resultFuture;
        this.descriptor = descriptor;
        this.fieldIndex = fieldIndex;
        this.statsManager = statsManager;
    }

    void start() {
        startTime = System.nanoTime();
    }

    @Override
    public void onSuccess(Response response) {
        ResponseBuilder responseBuilder = new ResponseBuilder(input);
        try {
            if (response.getStatusLine().getStatusCode() == 200) {
                statsManager.getCounter(SUCCESS_RESPONSE).inc();
                String responseBody = EntityUtils.toString(response.getEntity());
                enrichRow(responseBuilder, responseBody, descriptor);
            } else {
                statsManager.getMeter(FOUR_XX_RESPONSE).markEvent();
                System.err.println("ElasticSearch Service 4XX Error : Code : 404");
            }
        } catch (IOException e) {
            statsManager.getCounter(EXCEPTION).inc();
        } finally {
            statsManager.getHistogram(SUCCESS_RESPONSE_TIME).update(getElapsedTimeInMillis(startTime));
            resultFuture.complete(singleton(responseBuilder.build()));
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (e instanceof ResponseException) {
            statsManager.getCounter(FOUR_XX_RESPONSE).inc();
            System.err.println("ElasticSearch Service 4XX Error : Code : 4XX");
        } else {
            statsManager.getCounter(FIVE_XX_RESPONSE).inc();
            System.err.println("ElasticSearch Service 5XX Error : Code : 5XX");
        }
        statsManager.getCounter(EXCEPTION).inc();
        statsManager.getHistogram(FAILED_RESPONSE_TIME).update(getElapsedTimeInMillis(startTime));
        resultFuture.complete(singleton(input));
    }

    private void enrichRow(ResponseBuilder responseBuilder, String responseBody, Descriptor descriptor) {
        ObjectMapper objectMapper = new ObjectMapper();
        Row rowData = new Row(descriptor.getFields().size());
        try {
            Map<String, Object> map = objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {
            });
            rowData = makeRow((Map<String, Object>) (map.get("_source")), descriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            responseBuilder.with(fieldIndex, rowData);
        }
    }

    private long getElapsedTimeInMillis(long startTime) {
        return (System.nanoTime() - startTime) / 1000000;
    }
}
