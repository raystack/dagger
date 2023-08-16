package com.gotocompany.dagger.core.processors.external.http;

import com.google.protobuf.Descriptors;
import com.gotocompany.dagger.common.metrics.managers.MeterStatsManager;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandler;
import com.gotocompany.dagger.common.serde.typehandler.TypeHandlerFactory;
import com.gotocompany.dagger.core.exception.HttpFailureException;
import com.gotocompany.dagger.core.metrics.aspects.ExternalSourceAspects;
import com.gotocompany.dagger.core.metrics.reporters.ErrorReporter;
import com.gotocompany.dagger.core.processors.ColumnNameManager;
import com.gotocompany.dagger.core.processors.common.OutputMapping;
import com.gotocompany.dagger.core.processors.common.PostResponseTelemetry;
import com.gotocompany.dagger.core.processors.common.RowManager;
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
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The Http response handler.
 */
public class HttpResponseHandler extends AsyncCompletionHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpResponseHandler.class.getName());

    protected static final String SUCCESS_CODE_PATTERN = "^2.*";
    private final RowManager rowManager;
    private ColumnNameManager columnNameManager;
    private Descriptors.Descriptor descriptor;
    private ResultFuture<Row> resultFuture;
    private HttpSourceConfig httpSourceConfig;
    private Set<Integer> failOnErrorsExclusionSet;
    private MeterStatsManager meterStatsManager;
    private Instant startTime;
    private ErrorReporter errorReporter;
    private PostResponseTelemetry postResponseTelemetry;


    /**
     * Instantiates a new Http response handler.
     *
     * @param httpSourceConfig          the http source config
     * @param failOnErrorsExclusionSet  the fail on error exclusion set
     * @param meterStatsManager         the meter stats manager
     * @param rowManager                the row manager
     * @param columnNameManager         the column name manager
     * @param descriptor                the descriptor
     * @param resultFuture              the result future
     * @param errorReporter             the error reporter
     * @param postResponseTelemetry     the post response telemetry
     */
    public HttpResponseHandler(HttpSourceConfig httpSourceConfig, Set<Integer> failOnErrorsExclusionSet, MeterStatsManager meterStatsManager, RowManager rowManager,
                               ColumnNameManager columnNameManager, Descriptors.Descriptor descriptor, ResultFuture<Row> resultFuture,
                               ErrorReporter errorReporter, PostResponseTelemetry postResponseTelemetry) {

        this.httpSourceConfig = httpSourceConfig;
        this.failOnErrorsExclusionSet = failOnErrorsExclusionSet;
        this.meterStatsManager = meterStatsManager;
        this.rowManager = rowManager;
        this.columnNameManager = columnNameManager;
        this.descriptor = descriptor;
        this.resultFuture = resultFuture;
        this.errorReporter = errorReporter;
        this.postResponseTelemetry = postResponseTelemetry;
    }

    /**
     * Start timer.
     */
    public void startTimer() {
        startTime = Instant.now();
    }

    @Override
    public Object onCompleted(Response response) {
        int statusCode = response.getStatusCode();
        boolean isSuccess = Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(statusCode)).matches();
        if (isSuccess) {
            successHandler(response);
        } else {
            postResponseTelemetry.validateResponseCode(meterStatsManager, statusCode);
            failureHandler("Received status code : " + statusCode, statusCode);
        }
        return response;
    }

    @Override
    public void onThrowable(Throwable t) {
        t.printStackTrace();
        meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS);
        failureHandler(t.getMessage(), 0);
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
                postResponseTelemetry.failureReadingPath(meterStatsManager);
                LOGGER.error(e.getMessage());
                reportAndThrowError(e);
                return;
            }
            int fieldIndex = columnNameManager.getOutputIndex(key);
            setField(key, value, fieldIndex);
        });
        postResponseTelemetry.sendSuccessTelemetry(meterStatsManager, startTime);
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    /**
     * Failure handler.
     *
     * @param logMessage the log message
     * @param statusCode the status code
     */
    public void failureHandler(String logMessage, Integer statusCode) {
        postResponseTelemetry.sendFailureTelemetry(meterStatsManager, startTime);
        LOGGER.error(logMessage);
        Exception httpFailureException = new HttpFailureException(logMessage);
        if (shouldFailOnError(statusCode)) {
            reportAndThrowError(httpFailureException);
        } else {
            errorReporter.reportNonFatalException(httpFailureException);
        }
        resultFuture.complete(Collections.singleton(rowManager.getAll()));
    }

    private boolean shouldFailOnError(Integer statusCode) {
         if (httpSourceConfig.isFailOnErrors() && (statusCode == 0 || !failOnErrorsExclusionSet.contains(statusCode))) {
            return true;
        }
        return false;
    }

    private void setField(String key, Object value, int fieldIndex) {
        if (!httpSourceConfig.isRetainResponseType() || httpSourceConfig.hasType()) {
            setFieldUsingType(key, value, fieldIndex);
        } else {
            rowManager.setInOutput(fieldIndex, value);
        }
    }

    private void setFieldUsingType(String key, Object value, Integer fieldIndex) {
        Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(key);
        if (fieldDescriptor == null) {
            IllegalArgumentException illegalArgumentException = new IllegalArgumentException("Field Descriptor not found for field: " + key);
            reportAndThrowError(illegalArgumentException);
        }
        TypeHandler typeHandler = TypeHandlerFactory.getTypeHandler(fieldDescriptor);
        rowManager.setInOutput(fieldIndex, typeHandler.transformFromPostProcessor(value));
    }


    private void reportAndThrowError(Exception e) {
        errorReporter.reportFatalException(e);
        resultFuture.completeExceptionally(e);
    }
}
