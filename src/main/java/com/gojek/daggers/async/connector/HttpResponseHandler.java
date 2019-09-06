package com.gojek.daggers.async.connector;

import com.google.protobuf.Descriptors;
import com.jayway.jsonpath.JsonPath;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.gojek.daggers.Constants.EXTERNAL_SOURCE_OUTPUT_MAPPING;
import static com.gojek.daggers.Constants.EXTERNAL_SOURCE_OUTPUT_MAPPING_PATH;

public class HttpResponseHandler extends AsyncCompletionHandler<Object> {
    private Row input;
    private ResultFuture<Row> resultFuture;
    private Map<String, Object> configuration;
    private String[] columnNames;
    private Descriptors.Descriptor fieldDescriptor;

    public HttpResponseHandler(Row input, ResultFuture<Row> resultFuture, Map<String, Object> configuration,
                               String[] columnNames, Descriptors.Descriptor fieldDescriptor) {
        this.input = input;
        this.resultFuture = resultFuture;
        this.configuration = configuration;
        this.columnNames = columnNames;
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public Object onCompleted(Response response) throws Exception {
        if (response.getStatusCode() == 200 && response.hasResponseBody()) {
            Map<String, Object> outputMappings = (Map) configuration.get(EXTERNAL_SOURCE_OUTPUT_MAPPING);

            ArrayList<String> outputMappingKeys = new ArrayList<>(outputMappings.keySet());

            outputMappingKeys.forEach(key -> {
                Map<String, String> outputMappingKeyConfig = (Map) outputMappings.get(key);
                Float value = JsonPath.parse(response.getResponseBody()).read(outputMappingKeyConfig.get(EXTERNAL_SOURCE_OUTPUT_MAPPING_PATH), Float.class);
                Integer fieldIndex = Arrays.asList(columnNames).indexOf(key);
                input.setField(fieldIndex, value);
            });
        }
        resultFuture.complete(Collections.singleton(input));
        return response;
    }

    @Override
    public void onThrowable(Throwable t) {
        super.onThrowable(t);
    }
}
