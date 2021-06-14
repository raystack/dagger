package io.odpf.dagger.core.metrics.aspects;

import io.odpf.dagger.common.metrics.aspects.AspectType;
import io.odpf.dagger.common.metrics.aspects.Aspects;

/**
 * The enum External source aspects.
 */
public enum ExternalSourceAspects implements Aspects {
    CLOSE_CONNECTION_ON_EXTERNAL_CLIENT("close_connection_on_external_client", AspectType.Metric),
    FAILURES_ON_READING_PATH("failures_on_reading_path", AspectType.Metric),
    TOTAL_FAILED_REQUESTS("total_failures", AspectType.Metric),
    TIMEOUTS("timeouts", AspectType.Metric),
    SUCCESS_RESPONSE_TIME("success_response_time", AspectType.Histogram),
    FAILURES_RESPONSE_TIME("failure_response_time", AspectType.Histogram),
    SUCCESS_RESPONSE("success_response", AspectType.Metric),
    ERROR_PARSING_RESPONSE("parse_errors", AspectType.Metric),
    REQUEST_ERROR("request_error", AspectType.Metric),
    OTHER_ERRORS("other_errors", AspectType.Metric),
    TOTAL_EXTERNAL_CALLS("total_external_calls", AspectType.Metric),
    EMPTY_INPUT("empty_input", AspectType.Metric),
    ERROR_READING_RESPONSE("error_reading_response", AspectType.Metric),
    OTHER_ERRORS_PROCESSING_RESPONSE("other_errors_processing_response", AspectType.Metric),
    INVALID_CONFIGURATION("invalid_configuration", AspectType.Metric),
    FAILURE_CODE_5XX("failures_code5XX", AspectType.Metric),
    FAILURE_CODE_4XX("failures_code4XX", AspectType.Metric),
    FAILURE_CODE_404("failures_code404", AspectType.Metric),
    GRPC_CHANNEL_NOT_AVAILABLE("grpc_channel_not_available", AspectType.Metric);

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
