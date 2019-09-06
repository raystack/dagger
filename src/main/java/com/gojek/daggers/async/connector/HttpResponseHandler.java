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

import static com.gojek.daggers.Constants.EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY;
import static com.gojek.daggers.Constants.EXTERNAL_SOURCE_OUTPUT_MAPPING_PATH_KEY;

public class HttpResponseHandler extends AsyncCompletionHandler<Object> {
    private Row outputRow;
    private ResultFuture<Row> resultFuture;
    private Map<String, Object> configuration;
    private String[] columnNames;
    private Descriptors.Descriptor fieldDescriptor;

    public HttpResponseHandler(Row outputRow, ResultFuture<Row> resultFuture, Map<String, Object> configuration,
                               String[] columnNames, Descriptors.Descriptor fieldDescriptor) {
        this.outputRow = outputRow;
        this.resultFuture = resultFuture;
        this.configuration = configuration;
        this.columnNames = columnNames;
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public Object onCompleted(Response response) throws Exception {
        if (response.getStatusCode() == 200 && response.hasResponseBody()) {
            Map<String, Object> outputMappings = (Map) configuration.get(EXTERNAL_SOURCE_OUTPUT_MAPPING_KEY);

            ArrayList<String> outputMappingKeys = new ArrayList<>(outputMappings.keySet());


            outputMappingKeys.forEach(key -> {
                Map<String, String> outputMappingKeyConfig = (Map) outputMappings.get(key);
                Float value = JsonPath.parse(response.getResponseBody()).read(outputMappingKeyConfig.get(EXTERNAL_SOURCE_OUTPUT_MAPPING_PATH_KEY), Float.class);
                Integer fieldIndex = Arrays.asList(columnNames).indexOf(key);
                outputRow.setField(fieldIndex, value);
            });
        }
        resultFuture.complete(Collections.singleton(outputRow));
        return response;
    }

    @Override
    public void onThrowable(Throwable t) {
        super.onThrowable(t);
    }
}
