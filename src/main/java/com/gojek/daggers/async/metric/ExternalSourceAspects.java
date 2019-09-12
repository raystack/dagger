package com.gojek.daggers.async.metric;

import com.gojek.daggers.utils.stats.AspectType;
import com.gojek.daggers.utils.stats.Aspects;

import static com.gojek.daggers.utils.stats.AspectType.Histogram;
import static com.gojek.daggers.utils.stats.AspectType.Metric;

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
    SUCCESS_RESPONSE("successResponse", Metric);

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
