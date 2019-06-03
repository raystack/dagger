package com.gojek.daggers.async.metric;

import static com.gojek.daggers.async.metric.AspectType.*;

public enum Aspects {
    DOCUMENT_FOUND("documentFound",Metric),
    ERROR_PARSING_RESPONSE("parseErrors", Metric),
    SUCCESS_RESPONSE_TIME("successResponseTime", Histogram),
    FAILURES_ON_ES_RESPONSE_TIME("failedOnESResponseTime", Histogram),
    FAILURES_ON_ES("failuresOnES5XX",Metric),
    DOCUMENT_NOT_FOUND_ON_ES("documentNotFoundOnES404",Metric),
    REQUEST_ERROR("requestError",Metric),
    REQUEST_ERRORS_RESPONSE_TIME("requestErrorsResponseTime", Histogram),
    OTHER_ERRORS("otherErrors",Metric),
    OTHER_ERRORS_RESPONSE_TIME("otherErrorsResponseTime", Histogram),
    TOTAL_ES_CALLS("totalESCalls",Metric),
    TOTAL_FAILED_REQUESTS("totalFailures",Metric),
    EMPTY_INPUT("emptyInput",Metric),
    ERROR_READING_RESPONSE("errorReadingResponse",Metric),
    OTHER_ERRORS_PROCESSING_RESPONSE("otherErrorsProcessingResponse",Metric),
    FAILURES_ON_BIGTABLE_WRITE_DOCUMENT("failedOnBigtableWriteDocument",Metric),
    FAILURES_ON_BIGTABLE_CREATE_TABLE("failedOnBigtableCreateTable",Metric),
    TIMEOUTS("timeouts",Metric);

    private String value;
    private AspectType aspectType;

    Aspects(String value, AspectType aspectType) {
        this.value = value;
        this.aspectType = aspectType;
    }

    public String getValue() {
        return value;
    }

    public AspectType getAspectType() {
        return aspectType;
    }
}
