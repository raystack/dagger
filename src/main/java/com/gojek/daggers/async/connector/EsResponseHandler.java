package com.gojek.daggers.async.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gojek.daggers.builder.ResponseBuilder;
import com.google.gson.Gson;
import com.google.protobuf.Descriptors.Descriptor;
import com.timgroup.statsd.StatsDClient;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;

import java.io.IOException;
import java.util.Map;

import static com.gojek.daggers.utils.RowMaker.makeRow;
import static java.util.Collections.singleton;

public class EsResponseHandler implements ResponseListener {
    private static final String SUCCESS_ASPECT = "es.success.count";
    private static final String EXCEPTION_ASPECT = "es.failed.count";
    private static final String RESPONSE_ASPECT = "es.response.time";
    private static final String FOUR_XX_ASPECT = "es.4XX.failed.count";
    private static final String FIVE_XX_ASPECT = "es.5XX.failed.count";
//    private StatsDClient statsDClient;
    private Row input;
    private ResultFuture<Row> resultFuture;
    private long startTime;
    private Descriptor descriptor;
    private Integer fieldIndex;

    public EsResponseHandler(Row input, ResultFuture<Row> resultFuture, Descriptor descriptor, Integer fieldIndex) {
//        this.statsDClient = statsDClient;
        this.input = input;
        this.resultFuture = resultFuture;
        this.descriptor = descriptor;
        this.fieldIndex = fieldIndex;
    }

    void start() {
        startTime = System.nanoTime();
    }

    @Override
    public void onSuccess(Response response) {
        ResponseBuilder responseBuilder = new ResponseBuilder(input);
        try {
            if (response.getStatusLine().getStatusCode() == 200) {
//                statsDClient.increment(SUCCESS_ASPECT);
                String responseBody = EntityUtils.toString(response.getEntity());
                enrichRow(responseBuilder, responseBody, descriptor);
            } else {
//                statsDClient.increment(FOUR_XX_ASPECT);
                System.err.println("ElasticSearch Service 4XX Error : Code : 404");
            }
        } catch (IOException e) {
//            statsDClient.increment(EXCEPTION_ASPECT);
        } finally {
//            statsDClient.time(RESPONSE_ASPECT,
//                    getElapsedTimeInMillis(startTime));
            resultFuture.complete(singleton(responseBuilder.build()));
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (e instanceof ResponseException) {
//            statsDClient.increment(FOUR_XX_ASPECT);
            System.err.println("ElasticSearch Service 4XX Error : Code : 4XX");
        } else {
//            statsDClient.increment(FIVE_XX_ASPECT);
            System.err.println("ElasticSearch Service 5XX Error : Code : 5XX");
        }
//        statsDClient.increment(EXCEPTION_ASPECT);
//        statsDClient.time(RESPONSE_ASPECT, getElapsedTimeInMillis(startTime));
        resultFuture.complete(singleton(input));
    }

    private void enrichRow(ResponseBuilder responseBuilder, String responseBody, Descriptor descriptor) {
        ObjectMapper objectMapper = new ObjectMapper();
        Row rowData = new Row(descriptor.getFields().size());
        try {
            Map<String,Object> map = objectMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
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
