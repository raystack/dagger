package com.gojek.daggers.metrics.aspects;

import static com.gojek.daggers.metrics.aspects.AspectType.Histogram;
import static com.gojek.daggers.metrics.aspects.AspectType.Metric;

public enum ExternalSourceAspects implements Aspects {
    CLOSE_CONNECTION_ON_EXTERNAL_CLIENT("closeConnectionOnExternalClient", Metric),
    FAILURES_ON_READING_PATH("failuresOnReadingPath", Metric),
    TOTAL_FAILED_REQUESTS("totalFailures", Metric),
    TIMEOUTS("timeouts", Metric),
    SUCCESS_RESPONSE_TIME("successResponseTime", Histogram),
    FAILURES_RESPONSE_TIME("failureResponseTime", Histogram),
    SUCCESS_RESPONSE("successResponse", Metric),
    ERROR_PARSING_RESPONSE("parseErrors", Metric),
    FAILURES_ON_ES("failuresOnES5XX", Metric),
    REQUEST_ERROR("requestError", Metric),
    OTHER_ERRORS("otherErrors", Metric),
    TOTAL_EXTERNAL_CALLS("totalExternalCalls", Metric),
    EMPTY_INPUT("emptyInput", Metric),
    ERROR_READING_RESPONSE("errorReadingResponse", Metric),
    OTHER_ERRORS_PROCESSING_RESPONSE("otherErrorsProcessingResponse", Metric),
    INVALID_CONFIGURATION("invalidConfiguration", Metric),
    FAILURE_CODE_5XX("failuresCode5XX", Metric),
    FAILURE_CODE_4XX("failuresCode4XX", Metric),
    FAILURE_CODE_404("failuresCode404", Metric);

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
