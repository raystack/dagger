package com.gojek.daggers.postProcessors.external;

import com.gojek.daggers.metrics.MeterStatsManager;
import com.jayway.jsonpath.PathNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.*;
import static java.time.Duration.between;

public interface ResponseHandler {
    Logger LOGGER = LoggerFactory.getLogger(ResponseHandler.class.getName());

    default void sendSuccessTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(SUCCESS_RESPONSE);
        meterStatsManager.updateHistogram(SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    default void sendFailureTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(TOTAL_FAILED_REQUESTS);
        meterStatsManager.updateHistogram(FAILURES_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    default void failureReadingPath(MeterStatsManager meterStatsManager, PathNotFoundException exception) {
        meterStatsManager.markEvent(FAILURES_ON_READING_PATH);
    }

    default void validateResponseCode(MeterStatsManager meterStatsManager, int statusCode) {
        if (statusCode == 404) {
            meterStatsManager.markEvent(FAILURE_CODE_404);
        } else if (statusCode >= 400 && statusCode < 499) {
            meterStatsManager.markEvent(FAILURE_CODE_4XX);
        } else if (is5XX(statusCode)) {
            meterStatsManager.markEvent(FAILURE_CODE_5XX);
        } else {
            meterStatsManager.markEvent(OTHER_ERRORS);
        }
    }

    default boolean is5XX(int statusCode) {
        return statusCode >= 500 && statusCode < 599;
    }

}
