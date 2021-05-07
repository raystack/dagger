package io.odpf.dagger.core.processors.common;

import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;

import java.time.Instant;

import static io.odpf.dagger.core.utils.Constants.*;
import static java.time.Duration.between;

public class PostResponseTelemetry {

    public void sendSuccessTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(ExternalSourceAspects.SUCCESS_RESPONSE);
        meterStatsManager.updateHistogram(ExternalSourceAspects.SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    public void sendFailureTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        meterStatsManager.updateHistogram(ExternalSourceAspects.FAILURES_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    public void failureReadingPath(MeterStatsManager meterStatsManager) {
        meterStatsManager.markEvent(ExternalSourceAspects.FAILURES_ON_READING_PATH);
    }

    public void validateResponseCode(MeterStatsManager meterStatsManager, int statusCode) {
        if (statusCode == RESPONSE_CODE_404) {
            meterStatsManager.markEvent(ExternalSourceAspects.FAILURE_CODE_404);
        } else if (statusCode >= RESPONSE_CODE_400 && statusCode < RESPONSE_CODE_499) {
            meterStatsManager.markEvent(ExternalSourceAspects.FAILURE_CODE_4XX);
        } else if (statusCode >= RESPONSE_CODE_500 && statusCode < RESPONSE_CODE_599) {
            meterStatsManager.markEvent(ExternalSourceAspects.FAILURE_CODE_5XX);
        } else {
            meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS);
        }
    }
}
