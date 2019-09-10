package com.gojek.daggers.async.connector;

import com.gojek.daggers.async.metric.ExternalSourceAspects;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.gojek.daggers.Constants.*;
import static com.gojek.daggers.async.metric.ExternalSourceAspects.*;

public class HttpResponseHandler extends AsyncCompletionHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponseHandler.class.getName());
    private Row outputRow;
    private ResultFuture<Row> resultFuture;
    private Map<String, Object> configuration;
    private String[] columnNames;
    private Descriptors.Descriptor descriptor;
    private StatsManager statsManager;

    public HttpResponseHandler(Row outputRow, ResultFuture<Row> resultFuture, Map<String, Object> configuration,
                               String[] columnNames, Descriptors.Descriptor descriptor, StatsManager statsManager) {
        this.outputRow = outputRow;
        this.resultFuture = resultFuture;
        this.configuration = configuration;
        this.columnNames = columnNames;
        this.descriptor = descriptor;
        this.statsManager = statsManager;
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
        Map<String, Object> outputMappings = getValidatedMapField(configuration, EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY);
        ArrayList<String> outputMappingKeys = new ArrayList<>(outputMappings.keySet());

        outputMappingKeys.forEach(key -> {
            Map<String, String> outputMappingKeyConfig = getValidatedMapField(outputMappings, key);
            Object value = JsonPath.parse(response.getResponseBody()).read(outputMappingKeyConfig.get(EXTERNAL_SOURCE_OUTPUT_MAPPING_PATH_KEY), Object.class);
            Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(key);
            Integer fieldIndex = Arrays.asList(columnNames).indexOf(key);
            outputRow.setField(fieldIndex, RowMaker.fetchTypeAppropriateValue(value, fieldDescriptor));
        });
        statsManager.markEvent(SUCCESS_RESPONSE);
        resultFuture.complete(Collections.singleton(outputRow));
    }

    private void failureHandler(ExternalSourceAspects aspect, String logMessage) {
        statsManager.markEvent(aspect);
        LOGGER.error(logMessage);
        if ((boolean) configuration.getOrDefault(EXTERNAL_SOURCE_FAIL_ON_ERRORS_KEY, EXTERNAL_SOURCE_FAIL_ON_ERRORS_DEFAULT))
            throw new RuntimeException(logMessage);
        resultFuture.complete(Collections.singleton(outputRow));
    }

    private Map getValidatedMapField(Map<String, Object> map, String key) {
        Map resultMap;
        Object value = map.get(key);
        if (value == null)
            throw new IllegalArgumentException(key + " config should not be empty");
        try {
            resultMap = (Map) value;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(key + " config should be passed as a map");
        }
        return resultMap;
    }

}
