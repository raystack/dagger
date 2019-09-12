package com.gojek.daggers.async.connector;

import com.gojek.daggers.async.metric.ExternalSourceAspects;
import com.gojek.daggers.postprocessor.parser.HttpExternalSourceConfig;
import com.gojek.daggers.postprocessor.parser.OutputMapping;
import com.gojek.daggers.utils.RowMaker;
import com.gojek.daggers.utils.stats.StatsManager;
import com.google.protobuf.Descriptors;
import com.jayway.jsonpath.JsonPath;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.gojek.daggers.async.metric.ExternalSourceAspects.*;
import static java.time.Duration.between;

public class HttpResponseHandler extends AsyncCompletionHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponseHandler.class.getName());
    private Row outputRow;
    private ResultFuture<Row> resultFuture;
    private HttpExternalSourceConfig httpExternalSourceConfig;
    private String[] columnNames;
    private Descriptors.Descriptor descriptor;
    private StatsManager statsManager;
    private Instant startTime;

    public HttpResponseHandler(Row outputRow, ResultFuture<Row> resultFuture, HttpExternalSourceConfig httpExternalSourceConfig,
                               String[] columnNames, Descriptors.Descriptor descriptor, StatsManager statsManager) {
        this.outputRow = outputRow;
        this.resultFuture = resultFuture;
        this.httpExternalSourceConfig = httpExternalSourceConfig;
        this.columnNames = columnNames;
        this.descriptor = descriptor;
        this.statsManager = statsManager;
    }

    public void start() {
        startTime = Instant.now();
    }

    @Override
    public Object onCompleted(Response response) throws Exception {
        int statusCode = response.getStatusCode();
        if (statusCode == 200)
            successHandler(response);
        else if (statusCode >= 400 && statusCode < 499) {
            failureHandler(FAILURES_ON_HTTP_CALL_4XX, "Received status code : " + statusCode);
        } else if (statusCode >= 500 && statusCode < 599) {
            failureHandler(FAILURES_ON_HTTP_CALL_5XX, "Received status code : " + statusCode);
        } else
            failureHandler(FAILURES_ON_HTTP_CALL_OTHER_STATUS, "Received status code : " + statusCode);
        return response;
    }

    @Override
    public void onThrowable(Throwable t) {
        failureHandler(FAILURES_ON_HTTP_CALL_OTHER_ERRORS, t.getMessage());
    }

    private void successHandler(Response response) {
        Map<String, OutputMapping> outputMappings = httpExternalSourceConfig.getOutputMapping();
        ArrayList<String> outputMappingKeys = new ArrayList<>(outputMappings.keySet());

        outputMappingKeys.forEach(key -> {
            OutputMapping outputMappingKeyConfig = outputMappings.get(key);
            Object value = JsonPath.parse(response.getResponseBody()).read(outputMappingKeyConfig.getPath(), Object.class);
            Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(key);
            if (fieldDescriptor == null)
                completeExceptionally(resultFuture, new IllegalArgumentException("Field Descriptor not found for field: " + key));
            Integer fieldIndex = Arrays.asList(columnNames).indexOf(key);
            outputRow.setField(fieldIndex, RowMaker.fetchTypeAppropriateValue(value, fieldDescriptor));
        });
        statsManager.markEvent(SUCCESS_RESPONSE);
        statsManager.updateHistogram(SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        resultFuture.complete(Collections.singleton(outputRow));
    }

    private void failureHandler(ExternalSourceAspects aspect, String logMessage) {
        statsManager.updateHistogram(FAILURES_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        statsManager.markEvent(aspect);
        statsManager.markEvent(TOTAL_FAILED_REQUESTS);
        LOGGER.error(logMessage);
        if (httpExternalSourceConfig.isFailOnErrors())
            completeExceptionally(resultFuture, new RuntimeException(logMessage));
        resultFuture.complete(Collections.singleton(outputRow));
    }

    private void completeExceptionally(ResultFuture<Row> resultFuture, Exception exception) {
        resultFuture.completeExceptionally(exception);
    }

}
