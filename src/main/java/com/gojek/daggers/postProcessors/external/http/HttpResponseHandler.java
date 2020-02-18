package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.exception.HttpFailureException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.OutputMapping;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.protoHandler.ProtoHandler;
import com.gojek.daggers.protoHandler.ProtoHandlerFactory;
import com.google.protobuf.Descriptors;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static java.time.Duration.between;

public class HttpResponseHandler extends AsyncCompletionHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponseHandler.class.getName());
    private final RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Descriptors.Descriptor descriptor;
    private ResultFuture<Row> resultFuture;
    private HttpSourceConfig httpSourceConfig;
    private MeterStatsManager meterStatsManager;
    private Instant startTime;
    private ErrorReporter errorReporter;


    public HttpResponseHandler(HttpSourceConfig httpSourceConfig, MeterStatsManager meterStatsManager, RowManager rowManager,
                               ColumnNameManager columnNameManager, Descriptors.Descriptor descriptor, ResultFuture<Row> resultFuture,
                               ErrorReporter errorReporter) {

        this.httpSourceConfig = httpSourceConfig;
        this.meterStatsManager = meterStatsManager;
        this.rowManager = rowManager;
        this.columnNameManager = columnNameManager;
        this.descriptor = descriptor;
        this.resultFuture = resultFuture;
        this.errorReporter = errorReporter;
    }

    public void startTimer() {
        startTime = Instant.now();
    }

    @Override
    public Object onCompleted(Response response) {
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
        Map<String, OutputMapping> outputMappings = httpSourceConfig.getOutputMapping();
        ArrayList<String> outputMappingKeys = new ArrayList<>(outputMappings.keySet());

        outputMappingKeys.forEach(key -> {
            OutputMapping outputMappingKeyConfig = outputMappings.get(key);
            Object value;
            try {
                value = JsonPath.parse(response.getResponseBody()).read(outputMappingKeyConfig.getPath(), Object.class);
            } catch (PathNotFoundException e) {
                meterStatsManager.markEvent(FAILURES_ON_READING_PATH);
                LOGGER.error(e.getMessage());
                reportAndThrowError(e);
                return;
            }
            int fieldIndex = columnNameManager.getOutputIndex(key);
            setField(key, value, fieldIndex);
        });
        meterStatsManager.markEvent(SUCCESS_RESPONSE);
        meterStatsManager.updateHistogram(SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private void failureHandler(ExternalSourceAspects aspect, String logMessage) {
        meterStatsManager.updateHistogram(FAILURES_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
        meterStatsManager.markEvent(aspect);
        meterStatsManager.markEvent(TOTAL_FAILED_REQUESTS);
        LOGGER.error(logMessage);
        Exception httpFailureException = new HttpFailureException(logMessage);
        if (httpSourceConfig.isFailOnErrors()) {
            reportAndThrowError(httpFailureException);
        } else {
            errorReporter.reportNonFatalException(httpFailureException);
        }
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private void setField(String key, Object value, int fieldIndex) {
        if (httpSourceConfig.getType() == null) {
            rowManager.setInOutput(fieldIndex, value);
        } else {
            setFieldUsingType(key, value, fieldIndex);
        }
    }

    private void setFieldUsingType(String key, Object value, Integer fieldIndex) {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(key);
        if (fieldDescriptor == null) {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Field Descriptor not found for field: " + key);
            reportAndThrowError(illegalArgumentException);
        }
        ProtoHandler protoHandler = ProtoHandlerFactory.getProtoHandler(fieldDescriptor);
        rowManager.setInOutput(fieldIndex, protoHandler.transform(value));
    }

    private void reportAndThrowError(Exception e) {
        errorReporter.reportFatalException(e);
        resultFuture.completeExceptionally(e);
    }
}
