package com.gojek.daggers.postProcessors.external.es;

import com.gojek.daggers.exception.HttpFailureException;
import com.gojek.daggers.metrics.StatsManager;
import com.gojek.daggers.postProcessors.common.ColumnNameManager;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.gojek.daggers.metrics.AsyncAspects.*;
import static com.gojek.daggers.metrics.ExternalSourceAspects.FAILURES_ON_READING_PATH;
import static com.gojek.daggers.postProcessors.common.RowMaker.fetchTypeAppropriateValue;
import static com.gojek.daggers.postProcessors.common.RowMaker.makeRow;
import static java.time.Duration.between;
import static java.util.Collections.singleton;

public class EsResponseHandler implements ResponseListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(EsResponseHandler.class.getName());
    private EsSourceConfig esSourceConfig;
    private RowManager rowManager;
    private Descriptor outputDescriptor;
    private ResultFuture<Row> resultFuture;
    private Instant startTime;
    private StatsManager statsManager;
    private ColumnNameManager columnNameManager;

    public EsResponseHandler(EsSourceConfig esSourceConfig, StatsManager statsManager, RowManager rowManager, ColumnNameManager columnNameManager, Descriptor outputDescriptor, ResultFuture<Row> resultFuture) {
        this.esSourceConfig = esSourceConfig;
        this.rowManager = rowManager;
        this.outputDescriptor = outputDescriptor;
        this.resultFuture = resultFuture;
        this.statsManager = statsManager;
        this.columnNameManager = columnNameManager;
    }

    private static boolean isRetryStatus(ResponseException e) {
        int statusCode = e.getResponse().getStatusLine().getStatusCode();
        return statusCode == 502 || statusCode == 503 || statusCode == 504;
    }

    public void startTimer() {
        startTime = Instant.now();
    }

    @Override
    public void onSuccess(Response response) {
        try {
            if (response.getStatusLine().getStatusCode() != 200)
                return;
            statsManager.markEvent(DOCUMENT_FOUND);
            String responseBody = EntityUtils.toString(response.getEntity());
            List<String> esOutputColumnNames = esSourceConfig.getOutputColumns();
            esOutputColumnNames.forEach(outputColumnName -> {
                String outputColumnPath = esSourceConfig.getPath(outputColumnName);
                Object outputValue;
                try {
                    outputValue = JsonPath.parse(responseBody).read(outputColumnPath, new Object().getClass());
                } catch (PathNotFoundException exception) {
                    statsManager.markEvent(FAILURES_ON_READING_PATH);
                    LOGGER.error(exception.getMessage());
                    resultFuture.completeExceptionally(exception);
                    return;
                }
                int outputColumnIndex = columnNameManager.getOutputIndex(outputColumnName);
                setField(outputColumnIndex, outputValue, outputColumnName);
            });
        } catch (ParseException e) {
            statsManager.markEvent(ERROR_PARSING_RESPONSE);
            System.err.printf("ESResponseHandler : error parsing response, error msg : %s, response : %s\n", e.getMessage(), response.toString());
            e.printStackTrace();
        } catch (IOException e) {
            statsManager.markEvent(ERROR_READING_RESPONSE);
            System.err.printf("ESResponseHandler : error reading response, error msg : %s, response : %s\n", e.getMessage(), response.toString());
            e.printStackTrace();
        } catch (Exception e) {
            statsManager.markEvent(OTHER_ERRORS_PROCESSING_RESPONSE);
            System.err.printf("ESResponseHandler : other errors processing response, error msg : %s, response : %s\n", e.getMessage(), response.toString());
            e.printStackTrace();
        } finally {
            statsManager.updateHistogram(SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
            resultFuture.complete(singleton(rowManager.getAll()));
        }
    }

    @Override
    public void onFailure(Exception e) {
        statsManager.markEvent(TOTAL_FAILED_REQUESTS);
        if (esSourceConfig.isFailOnErrors())
            resultFuture.completeExceptionally(new HttpFailureException("EsResponseHandler : Failed with error. " + e.getMessage()));
        if (e instanceof ResponseException) {
            if (isRetryStatus((ResponseException) e)) {
                statsManager.markEvent(FAILURES_ON_ES);
                statsManager.updateHistogram(FAILURES_ON_ES_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
                System.err.printf("ESResponseHandler : all nodes unresponsive %s\n", e.getMessage());
            } else {
                if (isNotFound((ResponseException) e)) {
                    statsManager.markEvent(DOCUMENT_NOT_FOUND_ON_ES);
                } else {
                    statsManager.markEvent(REQUEST_ERROR);
                }
                statsManager.updateHistogram(REQUEST_ERRORS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
                System.err.printf("ESResponseHandler : request error %s\n", e.getMessage());
            }
        } else {
            statsManager.markEvent(OTHER_ERRORS);
            statsManager.updateHistogram(OTHER_ERRORS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
            System.err.printf("ESResponseHandler some other errors :  %s \n", e.getMessage());
        }
        resultFuture.complete(singleton(rowManager.getAll()));
    }

    private void setField(int index, Object value, String name) {
        if (!esSourceConfig.hasType()) {
            rowManager.setInOutput(index, value);
            return;
        }
        Descriptors.FieldDescriptor fieldDescriptor = outputDescriptor.findFieldByName(name);
        if (fieldDescriptor == null) {
            resultFuture.completeExceptionally(new IllegalArgumentException("Field Descriptor not found for field: " + name));
            statsManager.markEvent(INVALID_CONFIGURATION);
            return;
        }
        if (value instanceof Map) {
            rowManager.setInOutput(index, makeRow((Map<String, Object>) value, fieldDescriptor.getMessageType()));
        } else {
            rowManager.setInOutput(index, fetchTypeAppropriateValue(value, fieldDescriptor));
        }
    }

    private boolean isNotFound(ResponseException e) {
        return e.getResponse().getStatusLine().getStatusCode() == 404;
    }
}
