package io.odpf.dagger.core.processors.external;

import io.odpf.dagger.common.configuration.Configuration;
import io.odpf.dagger.core.metrics.telemetry.TelemetrySubscriber;
import io.odpf.dagger.core.utils.Constants;

import java.io.Serializable;

/**
 * The External metric config.
 */
public class ExternalMetricConfig implements Serializable {
    private final long shutDownPeriod;
    private final boolean telemetryEnabled;
    private TelemetrySubscriber telemetrySubscriber;
    private String metricId;

    /**
     * Instantiates a new External metric config.
     *
     * @param configuration       the configuration
     * @param telemetrySubscriber the telemetry subscriber
     */
    public ExternalMetricConfig(Configuration configuration, TelemetrySubscriber telemetrySubscriber) {
        this.shutDownPeriod = configuration.getLong(Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_KEY, Constants.METRIC_TELEMETRY_SHUTDOWN_PERIOD_MS_DEFAULT);
        this.telemetryEnabled = configuration.getBoolean(Constants.METRIC_TELEMETRY_ENABLE_KEY, Constants.METRIC_TELEMETRY_ENABLE_VALUE_DEFAULT);
        this.telemetrySubscriber = telemetrySubscriber;
    }

    /**
     * Instantiates a new External metric config with specified shutdown period.
     *
     * @param metricId         the metric id
     * @param shutDownPeriod   the shut down period
     * @param telemetryEnabled the telemetry enabled
     */
    public ExternalMetricConfig(String metricId, long shutDownPeriod, boolean telemetryEnabled) {
        this.metricId = metricId;
        this.shutDownPeriod = shutDownPeriod;
        this.telemetryEnabled = telemetryEnabled;
    }

    /**
     * Gets metric id.
     *
     * @return the metric id
     */
    public String getMetricId() {
        return metricId;
    }

    /**
     * Sets metric id.
     *
     * @param metricId the metric id
     */
    public void setMetricId(String metricId) {
        this.metricId = metricId;
    }

    /**
     * Gets telemetry subscriber.
     *
     * @return the telemetry subscriber
     */
    public TelemetrySubscriber getTelemetrySubscriber() {
        return telemetrySubscriber;
    }

    /**
     * Check if the telemetry is enabled.
     *
     * @return the boolean
     */
    public boolean isTelemetryEnabled() {
        return telemetryEnabled;
    }

    /**
     * Gets shut down period.
     *
     * @return the shut down period
     */
    public long getShutDownPeriod() {
        return shutDownPeriod;
    }
}
