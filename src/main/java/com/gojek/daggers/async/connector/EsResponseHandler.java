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
                statsManager.markEvent(DOCUMENT_FOUND);
                String responseBody = EntityUtils.toString(response.getEntity());
                enrichRow(responseBuilder, responseBody, descriptor);
            }
        } catch (Exception e) {
            statsManager.markEvent(ERROR_PARSING_RESPONSE);
            System.err.printf("ESResponseHandler : error parsing response %s\n", e.getMessage());
        } finally {
            statsManager.updateHistogram(SUCCESS_RESPONSE_TIME, getElapsedTimeInMillis(startTime));
            resultFuture.complete(singleton(responseBuilder.build()));
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (e instanceof ResponseException) {
            if (isRetryStatus((ResponseException) e)) {
                statsManager.markEvent(FAILURES_ON_ES);
                statsManager.updateHistogram(FAILURES_ON_ES_RESPONSE_TIME, getElapsedTimeInMillis(startTime));
                System.err.printf("ESResponseHandler : all nodes unresponsive %s\n", e.getMessage());
            } else {
                if (isNotFound((ResponseException) e)) {
                    statsManager.markEvent(DOCUMENT_NOT_FOUND_ON_ES);
                } else {
                    statsManager.markEvent(REQUEST_ERROR);
                }
                statsManager.updateHistogram(REQUEST_ERRORS_RESPONSE_TIME, getElapsedTimeInMillis(startTime));
                System.err.printf("ESResponseHandler : request error %s\n", e.getMessage());
            }
        } else {
            statsManager.markEvent(OTHER_ERRORS);
            //TimeoutException
            statsManager.updateHistogram(OTHER_ERRORS_RESPONSE_TIME, getElapsedTimeInMillis(startTime));
            System.err.printf("ESResponseHandler some other errors :  %s \n", e.getMessage());
        }
        statsManager.markEvent(TOTAL_FAILED_REQUESTS);
        resultFuture.complete(singleton(input));
    }

    private boolean isNotFound(ResponseException e) {
        return e.getResponse().getStatusLine().getStatusCode() == 404;
    }


    private static boolean isRetryStatus(ResponseException e) {
        int statusCode = e.getResponse().getStatusLine().getStatusCode();
        switch (statusCode) {
            case 502:
            case 503:
            case 504:
                return true;
        }
        return false;
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
