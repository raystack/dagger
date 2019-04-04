package com.gojek.daggers.async.connector.metric;

public enum Aspects {
    DOCUMENT_FOUND("documentFound"),
    ERROR_PARSING_RESPONSE("parse_errors"),
    SUCCESS_RESPONSE_TIME("successResponseTime"),
    FAILURES_ON_ES_RESPONSE_TIME("failedOnESResponseTime"),
    FAILURES_ON_ES("failuresOnES5XX"),
    DOCUMENT_NOT_FOUND_ON_ES("documentNotFoundOnES404"),
    REQUEST_ERROR("requestError"),
    REQUEST_ERRORS_RESPONSE_TIME("requestErrorsResponseTime"),
    OTHER_ERRORS("otherErrors"),
    OTHER_ERRORS_RESPONSE_TIME("otherErrorsResponseTime"),
    TOTAL_ES_CALLS("totalESCalls"),
    TOTAL_FAILED_REQUESTS("totalFailures"),
    EMPTY_INPUT("emptyInput");
    private String value;

    Aspects(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
