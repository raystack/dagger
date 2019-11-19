package com.gojek.daggers.metrics;

import static com.gojek.daggers.metrics.AspectType.Histogram;
import static com.gojek.daggers.metrics.AspectType.Metric;

public enum ExternalSourceAspects implements Aspects {
    CLOSE_CONNECTION_ON_HTTP_CLIENT("closeConnectionOnHttpClient", Metric),
    FAILURES_ON_HTTP_CALL_5XX("failuresOnHttp5XX", Metric),
    FAILURES_ON_HTTP_CALL_4XX("failuresOnHttp4XX", Metric),
    FAILURES_ON_HTTP_CALL_OTHER_STATUS("failuresOnHttpOtherStatus", Metric),
    FAILURES_ON_HTTP_CALL_OTHER_ERRORS("failuresOnHttpOtherErrors", Metric),
    FAILURES_ON_READING_PATH("failuresOnReadingPath", Metric),
    TOTAL_HTTP_CALLS("totalHttpCalls", Metric),
    TOTAL_FAILED_REQUESTS("totalFailures", Metric),
    TIMEOUTS("timeouts", Metric),
    SUCCESS_RESPONSE_TIME("successResponseTime", Histogram),
    FAILURES_RESPONSE_TIME("failureResponseTime", Histogram),
    SUCCESS_RESPONSE("successResponse", Metric),
    DOCUMENT_FOUND("documentFound", Metric),
    ERROR_PARSING_RESPONSE("parseErrors", Metric),
    FAILURES_ON_ES_RESPONSE_TIME("failedOnESResponseTime", Histogram),
    FAILURES_ON_ES("failuresOnES5XX", Metric),
    DOCUMENT_NOT_FOUND_ON_ES("documentNotFoundOnES404", Metric),
    REQUEST_ERROR("requestError", Metric),
    REQUEST_ERRORS_RESPONSE_TIME("requestErrorsResponseTime", Histogram),
    OTHER_ERRORS("otherErrors", Metric),
    OTHER_ERRORS_RESPONSE_TIME("otherErrorsResponseTime", Histogram),
    TOTAL_ES_CALLS("totalESCalls", Metric),
    EMPTY_INPUT("emptyInput", Metric),
    ERROR_READING_RESPONSE("errorReadingResponse", Metric),
    OTHER_ERRORS_PROCESSING_RESPONSE("otherErrorsProcessingResponse", Metric),
    INVALID_CONFIGURATION("invalidConfiguration", Metric);

    private String value;
    private AspectType aspectType;

    ExternalSourceAspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public AspectType getAspectType() {
        return aspectType;
    }
}
