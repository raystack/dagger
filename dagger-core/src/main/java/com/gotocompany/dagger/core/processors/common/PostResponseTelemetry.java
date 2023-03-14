package com.gotocompany.dagger.core.processors.common;

import com.gotocompany.dagger.common.metrics.managers.MeterStatsManager;
import com.gotocompany.dagger.core.metrics.aspects.ExternalSourceAspects;

import java.time.Instant;

import static com.gotocompany.dagger.core.utils.Constants.*;
import static java.time.Duration.between;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;

/**
 * The Post response telemetry.
 */
public class PostResponseTelemetry {

    /**
     * Send success telemetry.
     *
     * @param meterStatsManager the meter stats manager
     * @param startTime         the start time
     */
    public void sendSuccessTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(ExternalSourceAspects.SUCCESS_RESPONSE);
        meterStatsManager.updateHistogram(ExternalSourceAspects.SUCCESS_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    /**
     * Send failure telemetry.
     *
     * @param meterStatsManager the meter stats manager
     * @param startTime         the start time
     */
    public void sendFailureTelemetry(MeterStatsManager meterStatsManager, Instant startTime) {
        meterStatsManager.markEvent(ExternalSourceAspects.TOTAL_FAILED_REQUESTS);
        meterStatsManager.updateHistogram(ExternalSourceAspects.FAILURES_RESPONSE_TIME, between(startTime, Instant.now()).toMillis());
    }

    /**
     * Failure reading path.
     *
     * @param meterStatsManager the meter stats manager
     */
    public void failureReadingPath(MeterStatsManager meterStatsManager) {
        meterStatsManager.markEvent(ExternalSourceAspects.FAILURES_ON_READING_PATH);
    }

    /**
     * Validate response code.
     *
     * @param meterStatsManager the meter stats manager
     * @param statusCode        the status code
     */
    public void validateResponseCode(MeterStatsManager meterStatsManager, int statusCode) {
        if (statusCode == SC_NOT_FOUND) {
            meterStatsManager.markEvent(ExternalSourceAspects.FAILURE_CODE_404);
        } else if (isClientError(statusCode)) {
            meterStatsManager.markEvent(ExternalSourceAspects.FAILURE_CODE_4XX);
        } else if (isServerError(statusCode)) {
            meterStatsManager.markEvent(ExternalSourceAspects.FAILURE_CODE_5XX);
        } else {
            meterStatsManager.markEvent(ExternalSourceAspects.OTHER_ERRORS);
        }
    }

    private boolean isClientError(int statusCode) {
        return statusCode >= CLIENT_ERROR_MIN_STATUS_CODE && statusCode <= CLIENT_ERROR_MAX_STATUS_CODE;
    }
    private boolean isServerError(int statusCode) {
        return statusCode >= SERVER_ERROR_MIN_STATUS_CODE && statusCode <= SERVER_ERROR_MAX_STATUS_CODE;
    }
}
