package io.odpf.dagger.metrics.aspects;

public enum ExternalSourceAspects implements Aspects {
    CLOSE_CONNECTION_ON_EXTERNAL_CLIENT("closeConnectionOnExternalClient", AspectType.Metric),
    FAILURES_ON_READING_PATH("failuresOnReadingPath", AspectType.Metric),
    TOTAL_FAILED_REQUESTS("totalFailures", AspectType.Metric),
    TIMEOUTS("timeouts", AspectType.Metric),
    SUCCESS_RESPONSE_TIME("successResponseTime", AspectType.Histogram),
    FAILURES_RESPONSE_TIME("failureResponseTime", AspectType.Histogram),
    SUCCESS_RESPONSE("successResponse", AspectType.Metric),
    ERROR_PARSING_RESPONSE("parseErrors", AspectType.Metric),
    REQUEST_ERROR("requestError", AspectType.Metric),
    OTHER_ERRORS("otherErrors", AspectType.Metric),
    TOTAL_EXTERNAL_CALLS("totalExternalCalls", AspectType.Metric),
    EMPTY_INPUT("emptyInput", AspectType.Metric),
    ERROR_READING_RESPONSE("errorReadingResponse", AspectType.Metric),
    OTHER_ERRORS_PROCESSING_RESPONSE("otherErrorsProcessingResponse", AspectType.Metric),
    INVALID_CONFIGURATION("invalidConfiguration", AspectType.Metric),
    FAILURE_CODE_5XX("failuresCode5XX", AspectType.Metric),
    FAILURE_CODE_4XX("failuresCode4XX", AspectType.Metric),
    FAILURE_CODE_404("failuresCode404", AspectType.Metric),
    GRPC_CHANNEL_NOT_AVAILABLE("grpcChannelNotAvailable", AspectType.Metric);

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
