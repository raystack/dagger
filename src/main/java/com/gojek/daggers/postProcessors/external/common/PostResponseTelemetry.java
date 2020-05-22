package com.gojek.daggers.postProcessors.external.common;

import com.gojek.daggers.metrics.MeterStatsManager;

import java.time.Instant;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static java.time.Duration.between;

public class PostResponseTelemetry {

    public void sendSuccessTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(SUCCESS_RESPONSE);
        meterStatsManager.updateHistogram(SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    public void sendFailureTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(TOTAL_FAILED_REQUESTS);
        meterStatsManager.updateHistogram(FAILURES_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    public void failureReadingPath(MeterStatsManager meterStatsManager) {
        meterStatsManager.markEvent(FAILURES_ON_READING_PATH);
    }

    public void validateResponseCode(MeterStatsManager meterStatsManager, int statusCode) {
        if (statusCode == 404) {
            meterStatsManager.markEvent(FAILURE_CODE_404);
        } else if (statusCode >= 400 && statusCode < 499) {
            meterStatsManager.markEvent(FAILURE_CODE_4XX);
        } else if (statusCode >= 500 && statusCode < 599) {
            meterStatsManager.markEvent(FAILURE_CODE_5XX);
        } else {
            meterStatsManager.markEvent(OTHER_ERRORS);
        }
    }
}
